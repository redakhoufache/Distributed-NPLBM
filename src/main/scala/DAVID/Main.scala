package DAVID


import DAVID.Common.{IO, Tools}
import DAVID.Common.ProbabilisticTools.{sample, sampleWithSeed}
import DAVID.Common.Tools._
import DAVID.FunDisNPLBM.DisNPLBM
import DAVID.FunDisNPLBMRow.DisNPLBMRow
import breeze.linalg.{DenseMatrix, DenseVector, diag, sum}
import breeze.stats.distributions.{Gamma, MultivariateGaussian, RandBasis}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, lit, row_number}

import java.io.PrintStream
import java.io.FileOutputStream
/*
import java.io.File
*/
import scala.util.Random
class Plus(workerId:Int,
           row: List[(Int,List[DenseVector[Double]])], col: List[(Int,List[DenseVector[Double]])])extends Serializable{
  val id: Int = workerId
  val my_data: ( List[(Int,List[DenseVector[Double]])], List[(Int,List[DenseVector[Double]])]) = (row,col)
}
class Line(workerId:Int, data: List[(Int,List[DenseVector[Double]])])extends Serializable{
  val id:Int = workerId
  val my_data: List[(Int,List[DenseVector[Double]])]=data
  def getId:Int=id
  def getData(): List[(Int, List[DenseVector[Double]])] = my_data
}

object Main {
  def extractDouble(expectedNumber: Any):Array[Double]=expectedNumber.toString.split(":").map(_.toDouble)

  class ExactPartitioner(
                             partitions: Int,
                             elements: Int)
    extends Partitioner {

    override def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[Int]
      // `k` is assumed to go continuously from 0 to elements-1.
      k * partitions / elements
    }

    override def numPartitions: Int = partitions
  }
  def main(args: Array[String]) {
    /*----------------------------------------Spark_Conf------------------------------------------------*/
    val sparkMaster=args(0)
    val task_cores = args(12).toString
    val conf = new SparkConf().setMaster(sparkMaster).setAppName("DisNPLBM").set("spark.scheduler.mode", "FAIR").
      set("spark.task.cpus", task_cores)
    val sc = new SparkContext(conf)
                val shape = 1E1
                val scale = 2E1
    // Load datasets config file
    val datasetPath=args(6)
    val alphaPrior = Some(Gamma(shape = shape, scale = scale)) // lignes
                val betaPrior = Some(Gamma(shape = shape, scale = scale)) // clusters redondants
                /*val configDatasets = Common.IO.readConfigFromCsv("src/main/scala/dataset_glob.csv")*/
                val configDatasets = List(
                  Common.IO.readConfigFromCsv(s"$datasetPath/dataset_glob.csv")(args(5).toInt))
    val alpha: Option[Double] = None
    val beta: Option[Double] = None

    val actualAlphaPrior: Gamma =new Gamma(1D, 1D)
     /* alphaPrior match {
      case Some(g) => g
      case None => new Gamma(1D, 1D)
    }*/
    var actualBetaPrior: Gamma =new Gamma(1D, 1D)
     /* betaPrior match {
      case Some(g) => g
      case None => new Gamma(1D, 1D)
    }*/

    var actualAlpha: Double = alpha match {
      case Some(a) =>
        require(a > 0, s"AlphaRow parameter is optional and should be > 0 if provided, but got $a")
        a
      case None => actualAlphaPrior.mean
    }

    var actualBeta: Double = beta match {
      case Some(a) =>
        require(a > 0, s"AlphaCol parameter is optional and should be > 0 if provided, but got $a")
        a
      case None => actualBetaPrior.mean
    }

    def checkAlphaPrior(alpha: Option[Double], alphaPrior: Option[Gamma]): Boolean = {
      require(!(alpha.isEmpty & alphaPrior.isEmpty),
        "Either alphaRow or alphaRowPrior must be provided: please provide one of the two parameters.")
      require(!(alpha.isDefined & alphaPrior.isDefined),
        "Providing both alphaRow or alphaRowPrior is not supported: remove one of the two parameters.")
      alphaPrior.isDefined
    }

    var updateAlphaFlag: Boolean = checkAlphaPrior(alpha, alphaPrior)
    var updateBetaFlag: Boolean = checkAlphaPrior(beta, betaPrior)

    val alhpa_master = args(3).toDouble
    val alhpa_worker = args(4).toDouble

    println(configDatasets)
    val numberPartitions = args(1).toInt
    val nIter = args(2).toInt

    val verbose = false
    /*val nLaunches = 10*/
    val nLaunches = args(7).toInt

    configDatasets.foreach(dataset=>{
      val datasetName = dataset._1

      val trueRowPartitionSize = dataset._2
      val trueColPartitionSize = dataset._3
     /* val trueBlockPartition = Tools.blockPartition_row_col_size(trueRowPartitionSize, trueColPartitionSize)
      println(trueBlockPartition)*/
     val trueBlockPartition = List(0,0)/*getBlockPartition(getPartitionFromSize(trueRowPartitionSize),
       getPartitionFromSize(trueColPartitionSize))*/
      println(s"$datasetPath/data/$datasetName")
      
      val NPLBM = args(8).toInt
      val iterMaster = args(9).toInt
      val iterWorker = args(10).toInt
      val shuffle=args(11).toBoolean
      /*val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
      val dataList = spark.read.option("header", "true")
        .csv(s"$datasetPath/data/$datasetName")
        .rdd.map(_.toSeq.toList.map(elem => DenseVector(extractDouble(elem)))).collect().toList.transpose*/
      val dataList=scala.io.Source.fromFile(s"$datasetPath/data/$datasetName").getLines().drop(1).map(_.split(",").map(_.toDouble).map(DenseVector(_)).toList).toList.transpose
      if(shuffle){
        val N=dataList.size
        val P=dataList.head.size
        var shuffled_dataList=List.fill(N)(List.fill(P)(DenseVector(0.0,0.0)))
        val map_shuffled_realRow=((0 until N) zip Random.shuffle((0 until N).toList)).toList
        val map_shuffled_realCol=((0 until P) zip Random.shuffle((0 until P).toList)).toList
      }
      val dataByColRDD = sc.parallelize(dataList.zipWithIndex.map(e => (e._2, e._1)), numberPartitions )
      val dataByRowRDD = sc.parallelize((dataList.transpose).zipWithIndex.map(e => (e._2, e._1)), numberPartitions)
      val workerRowRDD = dataByRowRDD.mapPartitionsWithIndex((index, data) => {
        Iterator(new Line(index, data.toList))
      })
/*      val workerRowRDDList=workerRowRDD.collect().toList
      val workerRDD = dataByColRDD.mapPartitionsWithIndex((index, data) => {
        val col = data.toList
        val row = workerRowRDDList(index).my_data
        Iterator(new Plus(index, row, col))
      })*/
      val workerRDD =sc.parallelize(List(new Plus(0,workerRowRDD.first().my_data,workerRowRDD.first().my_data)))
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
              betaPrior = betaPrior).run(verbose = verbose,nIter = nIter-10)
            val t1 = printTime(t0, "NPLBM")
            val blockPartition = getBlockPartition(rowMembershipNPLBM.last, colMembershipNPLBM.last)
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
                dataRDD = workerRDD, actualAlpha = actualAlpha,
                actualBeta = actualBeta,
                masterAlphaPrior = alhpa_master, workerAlphaPrior = alhpa_worker).run(maxIter = nIter,
                maxIterMaster = iterMaster, maxIterWorker = iterWorker)
              val t1 = printTime(t0, "Dis_NPLBM")
              val blockPartition = getBlockPartition(rowMembershipDis_NPLBM, colMembershipDis_NPLBM)
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
              val (rowMembershipDis_NPLBM, colMembershipDis_NPLBM) = new DisNPLBMRow(master = sparkMaster,
                dataRDD = workerRowRDD, actualAlpha = actualAlpha,
                actualBeta = actualBeta).run(maxIter = nIter,
                maxIterMaster = iterMaster, maxIterWorker = iterWorker)
              val t1 = printTime(t0, "Dis_NPLBMRow")
              System.out.println("rowMembershipDis_NPLBM=", rowMembershipDis_NPLBM)
              System.out.println("colMembershipDis_NPLBM=", colMembershipDis_NPLBM)
              
              val blockPartition = List(0,0)/*getBlockPartition(rowMembershipDis_NPLBM, colMembershipDis_NPLBM)*/
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

