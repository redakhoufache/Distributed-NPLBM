package DAVID


import DAVID.Common.DataGeneration.{generate_20k_10k_10K_3L, generate_reda, randomLBMDataGeneration}
import DAVID.Common.{IO, NormalInverseWishart, Tools}
import DAVID.Common.Tools._
import DAVID.FunDisNPLBM.DisNPLBM
import DAVID.FunDisNPLBMRow.DisNPLBMRow
import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.Gamma
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import java.io.PrintStream
import java.io.FileOutputStream

class Line(workerId:Int, data: List[(Int,List[DenseVector[Double]])])extends Serializable{
  val id:Int = workerId
  val my_data: List[(Int,List[DenseVector[Double]])]=data
  def getId:Int=id
  def getData(): List[(Int, List[DenseVector[Double]])] = my_data
}

object Main {
  def main(args: Array[String]) {
    /*----------------------------------------Spark_Conf------------------------------------------------*/

    val sparkMaster=args(0)
    val numberPartitions = args(1).toInt
    val nIter = args(2).toInt
    val shapeInt = args(3).toDouble
    val scaleInt = args(4).toDouble
    val index_dataset=args(5).toInt
    val datasetPath=args(6)
    val nLaunches = args(7).toInt
    val NPLBM = args(8).toInt
    val iterMaster = args(9).toInt
    val iterWorker = args(10).toInt
    val shuffle = args(11).toBoolean
    val task_cores = args(12).toString
    val dim = args(13).toInt
    val verbose = args(14).toBoolean
    val score= args(15).toBoolean
    val likelihood= args(16).toBoolean
    val generated= args(17).toBoolean
    val alphaUser = args(18).toDouble
    val betaUser = args(19).toDouble
    println("verbose",verbose)
    if(verbose){
      println("Ok")
    }
    if (generated) generate_reda(datasetPath)
    val conf = new SparkConf().setMaster(sparkMaster).setAppName("DisNPLBM").set("spark.scheduler.mode", "FAIR").
      set("spark.task.cpus", task_cores)
    val sc = new SparkContext(conf)
    val shape = shapeInt
    val scale = scaleInt
    // Load datasets config file

    val alphaPrior = Some(Gamma(shape = shape, scale = scale)) // lignes
    val betaPrior = Some(Gamma(shape = shape, scale = scale)) // clusters redondants
                /*val configDatasets = Common.IO.readConfigFromCsv("src/main/scala/dataset_glob.csv")*/
                val configDatasets = List(
                  Common.IO.readConfigFromCsv(s"$datasetPath/dataset_glob.csv")(index_dataset))
    val alpha: Option[Double] = None
    val beta: Option[Double] = None

    configDatasets.foreach(dataset=>{
      var datasetName = dataset._1

      val trueRowPartitionSize =  dataset._2
      val trueColPartitionSize = dataset._3
      var row_flaten_label = getPartitionFromSize(trueRowPartitionSize)
      var col_flaten_label = getPartitionFromSize(trueColPartitionSize)
     /* val trueBlockPartition = Tools.blockPartition_row_col_size(trueRowPartitionSize, trueColPartitionSize)
      println(trueBlockPartition)*/
      if (shuffle) {
        datasetName = s"${datasetName.dropRight(4)}_Shuffled.csv"
      }
     val trueBlockPartition = if(shuffle) {
       val shuffled_datasetName=datasetName.split("_")
       val lines_scores=scala.io.Source.fromFile(s"$datasetPath/data/label_${shuffled_datasetName(1)}_${shuffled_datasetName(2)}_${shuffled_datasetName(3)}_${shuffled_datasetName(4)}").getLines().toList
       val trueLabelRow=lines_scores(0).split(",").map(_.toInt)
       val trueLabelCol=lines_scores(1).split(",").map(_.toInt)
       val row_flaten_label_shuffle=trueLabelRow.map(row_flaten_label(_)).toList
       val col_flaten_label_shuffle=trueLabelCol.map(col_flaten_label(_)).toList
       row_flaten_label=row_flaten_label_shuffle
       col_flaten_label=col_flaten_label_shuffle
       if (score) getBlockPartition(row_flaten_label_shuffle, col_flaten_label_shuffle) else List(0, 0)
     }else {
       if (score) getBlockPartition(row_flaten_label,col_flaten_label) else List(0,0)
     }


      /*val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
      val dataList = spark.read.option("header", "true")
        .csv(s"$datasetPath/data/$datasetName")
        .rdd.map(_.toSeq.toList.map(elem => DenseVector(extractDouble(elem)))).collect().toList.transpose*/

      val dataList =scala.io.Source.fromFile(s"$datasetPath/data/$datasetName").getLines().drop(1).toList.par.
        map(_.split(",").map(e => {
          if (dim == 1) {
            Array(e.toDouble)
          } else {
            e.split(":").map(k => k.toDouble)
          }
        }).map(e => DenseVector(e)).toList).toList.transpose
      val dataByColRDD = sc.parallelize(dataList.zipWithIndex.map(e => (e._2, e._1)), numberPartitions )
      val dataByRowRDD = sc.parallelize((dataList.transpose).zipWithIndex.map(e => (e._2, e._1)), numberPartitions)
      val workerRowRDD = dataByRowRDD.mapPartitionsWithIndex((index, data) => {
        Iterator(new Line(index, data.toList))
      })
      val workerRDDcol = dataByColRDD.mapPartitionsWithIndex((index, data) => {
        Iterator(new Line(index, data.toList))
      })
      /*val workerRDD =sc.parallelize(List(new Plus(0,workerRowRDD.first().my_data,workerRowRDD.first().my_data)))*/
      if (NPLBM==1){
        System.setOut(new PrintStream(
          new FileOutputStream(s"$datasetPath/result/file_${datasetName}_${numberPartitions}_NPLBM.out")))
      } else {
        if (NPLBM == 0) {
          System.setOut(new PrintStream(
            new FileOutputStream(s"$datasetPath/result/file_${datasetName}_${numberPartitions}_Dis_NPLBM.out")))
            System.out.println(s"num_threads=${Runtime.getRuntime.availableProcessors()} " +
      s"num_cores=${Runtime.getRuntime.availableProcessors()/2}")
          System.out.println("number of row parations->", dataByRowRDD.getNumPartitions)
          System.out.println("number of col parations->", dataByColRDD.getNumPartitions)}
        else{
          System.setOut(new PrintStream(
            new FileOutputStream(s"$datasetPath/result/file_${datasetName}_${numberPartitions}_Dis_NPLBMRow.out")))
            System.out.println(s"num_threads=${Runtime.getRuntime.availableProcessors()} " +
      s"num_cores=${Runtime.getRuntime.availableProcessors()/2}")
          System.out.println("number of row parations->", dataByRowRDD.getNumPartitions)
          System.out.println("number of col parations->", dataByColRDD.getNumPartitions)
        }
      }

      /*--------------------------------------------------------------------------------------------------*/
      println("Benchmark begins. It is composed of " + nLaunches.toString +
        " launches, each launch runs every methods once.")

      (0 until nLaunches).foreach(iter => {
        System.out.println("Launch number " + iter)

        if (NPLBM==1){
          //////////////////////////////////// NPLBM
          val ((ariNPLBM, riNPLBM, nmiNPLBM, nClusterNPLBM), runtimeNPLBM) = {
            val t0 = System.nanoTime()
            val (rowMembershipNPLBM, colMembershipNPLBM) = new DAVID.FunNPLBM.CollapsedGibbsSampler(dataList,
              alphaPrior = alphaPrior,
              betaPrior = betaPrior,trueBlockPartition = trueBlockPartition).run(verbose = verbose,nIter = nIter-10)
            val t1 = printTime(t0, "NPLBM")
            System.out.println("rowMembership_NPLBM=", rowMembershipNPLBM.last)
            System.out.println("colMembership_NPLBM=", colMembershipNPLBM.last)
            val blockPartition = if (score) getBlockPartition(rowMembershipNPLBM.last, colMembershipNPLBM.last) else List(0,0)
            (getScores(blockPartition, trueBlockPartition), (t1 - t0) / 1e9D)
          }
          System.out.println("ariNPLBM=", ariNPLBM)
          System.out.println("riNPLBM=", riNPLBM)
          System.out.println("nmiNPLBM=", nmiNPLBM)
          System.out.println("nClusterNPLBM=", nClusterNPLBM)
          System.out.println("runtimeNPLBM=", runtimeNPLBM)
        }else {
          if(NPLBM==0){
            //////////////////////////////////// Dis_NPLBM
            val ((ariDis_NPLBM, riDis_NPLBM, nmiDis_NPLBM, nClusterDis_NPLBM), runtimeDis_NPLBM) = {
              val t0 = System.nanoTime()
              val (rowMembershipDis_NPLBM, colMembershipDis_NPLBM) = new DisNPLBM(master = sparkMaster,
                dataRDDrow = workerRowRDD,dataRDDcol = workerRDDcol, alphaPrior = alphaPrior,
                betaPrior = betaPrior).run(maxIter = nIter,
                maxIterMaster = iterMaster, maxIterWorker = iterWorker)
              val t1 = printTime(t0, "Dis_NPLBM")
              System.out.println("rowMembershipDis_NPLBM=", rowMembershipDis_NPLBM)
              System.out.println("colMembershipDis_NPLBM=", colMembershipDis_NPLBM)
              val blockPartition = if (score) getBlockPartition(rowMembershipDis_NPLBM, colMembershipDis_NPLBM) else List(0, 0)
              (getScores(blockPartition, trueBlockPartition), (t1 - t0) / 1e9D)
            }
            System.out.println("ariDis_NPLBM=", ariDis_NPLBM)
            System.out.println("riDis_NPLBM=", riDis_NPLBM)
            System.out.println("nmiDis_NPLBM=", nmiDis_NPLBM)
            System.out.println("nClusterDis_NPLBM=", nClusterDis_NPLBM)
            System.out.println("runtimeDis_NPLBM=", runtimeDis_NPLBM)

          }else{
            //////////////////////////////////// Dis_NPLBMRow
            val ((ariDis_NPLBMRow, riDis_NPLBMRow, nmiDis_NPLBMRow, nClusterDis_NPLBMRow), runtimeDis_NPLBMRow) = {
              val t0 = System.nanoTime()
              val (rowMembershipDis_NPLBM, colMembershipDis_NPLBM) =if (likelihood) {new DisNPLBMRow(master = sparkMaster,
                dataRDD = workerRowRDD, alphaPrior = alphaPrior,
                betaPrior = betaPrior,initByUserPrior = Some(new NormalInverseWishart(dataList)),
                score = score,likelihood = likelihood,alldata = dataList,trueBlockPartition = trueBlockPartition)
                .run(maxIter = nIter, maxIterMaster = iterMaster, maxIterWorker = iterWorker)} else{
                new DisNPLBMRow(master = sparkMaster, dataRDD = workerRowRDD, alphaPrior = alphaPrior, betaPrior = betaPrior
                  , initByUserPrior = Some(new NormalInverseWishart(dataList)),
                  score = score,trueBlockPartition = trueBlockPartition).run(maxIter = nIter,
                  maxIterMaster = iterMaster, maxIterWorker = iterWorker)
              }
              val t1 = printTime(t0, "Dis_NPLBMRow")
              System.out.println("rowMembershipDis_NPLBM=", rowMembershipDis_NPLBM)
              System.out.println("colMembershipDis_NPLBM=", colMembershipDis_NPLBM)
              
              val blockPartition = if (score) getBlockPartition(rowMembershipDis_NPLBM, colMembershipDis_NPLBM) else List(0, 0)
              (getScores(blockPartition, trueBlockPartition), (t1 - t0) / 1e9D)
            }
            System.out.println("ariDis_NPLBMRow=", ariDis_NPLBMRow)
            System.out.println("riDis_NPLBMRow=", riDis_NPLBMRow)
            System.out.println("nmiDis_NPLBMRow=", nmiDis_NPLBMRow)
            System.out.println("nClusterDis_NPLBMRow=", nClusterDis_NPLBMRow)
            System.out.println("runtimeDis_NPLBMRow=", runtimeDis_NPLBMRow)
          }

        }

        /*val ARIs = Array(shape, scale, ariNPLBM, ariDis_NPLBM)
        val RIs = Array(shape, scale, riNPLBM, riDis_NPLBM)
        val NMIs = Array(shape, scale, nmiNPLBM, nmiDis_NPLBM)
        val nClusters = Array(shape, scale, nClusterNPLBM, nClusterDis_NPLBM)
        val runtimes = Array(shape, scale, runtimeNPLBM, runtimeDis_NPLBM)


        val ARIMat = DenseMatrix(ARIs.map(_.toString)).reshape(1, ARIs.length)
        val RIMat = DenseMatrix(RIs.map(_.toString)).reshape(1, ARIs.length)
        val NMIMat = DenseMatrix(NMIs.map(_.toString)).reshape(1, ARIs.length)
        val nClusterMat = DenseMatrix(nClusters.map(_.toString)).reshape(1, ARIs.length)
        val runtimesMat = DenseMatrix(runtimes.map(_.toString)).reshape(1, ARIs.length)

        val append = true

        IO.writeMatrixStringToCsv(
          s"$datasetPath/result/${datasetName.dropRight(4)}_${numberPartitions}_${iter}_ARIs.csv",
          ARIMat, append = append)
        IO.writeMatrixStringToCsv(
          s"$datasetPath/result/${datasetName.dropRight(4)}_${numberPartitions}_RIs.csv",
          RIMat, append = append)
        IO.writeMatrixStringToCsv(
          s"$datasetPath/result/${datasetName.dropRight(4)}_${numberPartitions}_NMIs.csv",
          NMIMat, append = append)
        IO.writeMatrixStringToCsv(
          s"$datasetPath/result/${datasetName.dropRight(4)}_${numberPartitions}_nClusters.csv",
          nClusterMat, append = append)
        IO.writeMatrixStringToCsv(
          s"$datasetPath/result/${datasetName.dropRight(4)}_${numberPartitions}_runtimes.csv",
          runtimesMat, append = append)*/

      })
    })
    }

       }

