package DAVID


import DAVID.Common.IO
import DAVID.Common.Tools._
import DAVID.FunDisNPLBM.DisNPLBM
import DAVID.FunLBM.ModelSelection
import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.Gamma
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
class Plus(workerId:Int, row: List[(Int,List[DenseVector[Double]])], col: List[(Int,List[DenseVector[Double]])])extends Serializable{
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

  def main(args: Array[String]) {

                val shape = 1E1
                val scale = 2E1

                val alphaPrior = Some(Gamma(shape = shape, scale = scale)) // lignes
                val betaPrior = Some(Gamma(shape = shape, scale = scale)) // clusters redondants

                val trueRowPartitionSize = List(20,30,40,30,20)
                val trueColPartitionSize = List(40,20,30,20,30)
                val trueBlockPartition = getBlockPartition(getPartitionFromSize(trueRowPartitionSize),
                        getPartitionFromSize(trueColPartitionSize))

                val dataMatrix = Common.IO.readDenseMatrixDvDouble("src/main/scala/dataset.csv")
                val dataList = matrixToDataByCol(dataMatrix)

                val nIter = args(2).toInt
                val verbose = false
                /*val nLaunches = 10*/
                val nLaunches = 1
    val alhpa_master=args(2).toDouble
    val alhpa_worker=args(3).toDouble

    val numberWorkers=args(1).toInt
                val sparkMaster=args(0)
                /*----------------------------------------Spark_Conf------------------------------------------------*/
                val conf = new SparkConf().setMaster(sparkMaster).setAppName("DisNPLBM")
                val sc = new SparkContext(conf)
                val dataByColRDD = sc.parallelize(dataList.zipWithIndex.map(e => (e._2, e._1)), numberWorkers * 1)
                val dataByRowRDD = sc.parallelize((dataList.transpose).zipWithIndex.map(e => (e._2, e._1)), numberWorkers * 1)
                val workerRowRDD = dataByRowRDD.mapPartitionsWithIndex((index, data) => {
                  Iterator(new Line(index, data.toList))
                }).collect().toList
                val workerRDD = dataByColRDD.mapPartitionsWithIndex((index, data) => {
                  val col = data.toList
                  val row = workerRowRDD(index).my_data
                  Iterator(new Plus(index, row, col))
                })
                /*--------------------------------------------------------------------------------------------------*/
                println("Benchmark begins. It is composed of "+ nLaunches.toString + " launches, each launch runs every methods once.")

                (0 until nLaunches).foreach(iter => {

                        var t0 = System.nanoTime()

                        println("Launch number " + iter)

                      /*//////////////////////////////////// BGMM_1

                      val ((ariBGMM1, riBGMM1, nmiBGMM1, nClusterBGMM1), runtimeBGMM1) = {

                        var t0 = System.nanoTime()

                        val bestBGMMRow = ModelSelection.gridSearch(
                          dataMatrix,
                          rangeRow = List(5),
                          rangeCol = List(1),
                          verbose = verbose,
                          nConcurrentEachTest = 1,
                          nTryMaxPerConcurrent = 1)
                        val bestBGMMRowPartition = bestBGMMRow("RowPartition").asInstanceOf[List[Int]]

                        val bestBGMMCol = FunLBM.ModelSelection.gridSearch(
                          dataMatrix,
                          rangeRow = List(1),
                          rangeCol = List(5),
                          verbose = verbose,
                          nConcurrentEachTest = 1,
                          nTryMaxPerConcurrent = 1)
                        val bestBGMMColPartition = bestBGMMCol("ColPartition").asInstanceOf[List[Int]]

                        val t1 = printTime(t0, "BGMM 1")

                        val blockPartition = getBlockPartition(bestBGMMRowPartition, bestBGMMColPartition)
                        (getScores(blockPartition, trueBlockPartition), (t1 - t0)/1e9D)
                      }

                      //////////////////////////////////// BGMM_14

                      val ((ariBGMM14, riBGMM14, nmiBGMM14, nClusterBGMM14), runtimeBGMM14) = {

                        var t0 = System.nanoTime()

                        val bestBGMMRow = FunLBM.ModelSelection.gridSearch(
                          dataMatrix,
                          rangeRow = List(5),
                          rangeCol = List(1),
                          verbose = verbose,
                          nConcurrentEachTest = 4,
                          nTryMaxPerConcurrent = 1)
                        val bestBGMMRowPartition = bestBGMMRow("RowPartition").asInstanceOf[List[Int]]

                        val bestBGMMCol = FunLBM.ModelSelection.gridSearch(
                          dataMatrix,
                          rangeRow = List(1),
                          rangeCol = List(5),
                          verbose = verbose,
                          nConcurrentEachTest = 4,
                          nTryMaxPerConcurrent = 1)
                        val bestBGMMColPartition = bestBGMMCol("ColPartition").asInstanceOf[List[Int]]

                        val t1 = printTime(t0, "BGMM 14")

                        val blockPartition = getBlockPartition(bestBGMMRowPartition, bestBGMMColPartition)
                        (getScores(blockPartition, trueBlockPartition), (t1 - t0)/1e9D)
                      }

                      //////////////////////////////////// BGMM_MS

                      val ((ariBGMM_MS, riBGMM_MS, nmiBGMM_MS, nClusterBGMM_MS), runtimeBGMM_MS) = {

                        var t0 = System.nanoTime()

                        val bestBGMMRow = FunLBM.ModelSelection.gridSearch(
                          dataMatrix,
                          rangeRow = List(4, 5, 6),
                          rangeCol = List(1),
                          verbose = verbose,
                          nConcurrentEachTest = 1,
                          nTryMaxPerConcurrent = 1)
                        val bestBGMMRowPartition = bestBGMMRow("RowPartition").asInstanceOf[List[Int]]

                        val bestBGMMCol = FunLBM.ModelSelection.gridSearch(
                          dataMatrix,
                          rangeRow = List(1),
                          rangeCol = List(4, 5, 6),
                          verbose = verbose,
                          nConcurrentEachTest = 1,
                          nTryMaxPerConcurrent = 1)
                        val bestBGMMColPartition = bestBGMMCol("ColPartition").asInstanceOf[List[Int]]

                        val t1 = printTime(t0, "BGMM MS")

                        val blockPartition = getBlockPartition(bestBGMMRowPartition, bestBGMMColPartition)
                        (getScores(blockPartition, trueBlockPartition), (t1 - t0)/1e9D)
                      }

                      //////////////////////////////////// LBM1

                      val ((ariLBM1, riLBM1, nmiLBM1, nClusterLBM1), runtimeLBM1) = {
                        var t0 = System.nanoTime()
                        val bestLBM = FunLBM.ModelSelection.gridSearch(
                          dataMatrix,
                          rangeRow = List(5),
                          rangeCol = List(5),
                          verbose = verbose,
                          nConcurrentEachTest = 1,
                          nTryMaxPerConcurrent = 5)
                        val (rowMembershipLBM, colMembershipLBM) = (
                          bestLBM("RowPartition").asInstanceOf[List[Int]],
                          bestLBM("ColPartition").asInstanceOf[List[Int]])
                        val t1 = printTime(t0, "LBM 1")

                        val blockPartition = getBlockPartition(rowMembershipLBM, colMembershipLBM)
                        (getScores(blockPartition, trueBlockPartition), (t1 - t0)/1e9D)
                      }

                      //////////////////////////////////// LBM49

                      val ((ariLBM49, riLBM49, nmiLBM49, nClusterLBM49), runtimeLBM49) = {
                        var t0 = System.nanoTime()
                        val bestLBM = FunLBM.ModelSelection.gridSearch(
                          dataMatrix,
                          rangeRow = List(5),
                          rangeCol = List(5),
                          verbose = verbose,
                          nConcurrentEachTest = 10,
                          nTryMaxPerConcurrent = 5)
                        val (rowMembershipLBM, colMembershipLBM) = (
                          bestLBM("RowPartition").asInstanceOf[List[Int]],
                          bestLBM("ColPartition").asInstanceOf[List[Int]])
                        val t1 = printTime(t0, "LBM 49")

                        val blockPartition = getBlockPartition(rowMembershipLBM, colMembershipLBM)
                        (getScores(blockPartition, trueBlockPartition), (t1 - t0)/1e9D)
                      }

                      //////////////////////////////////// LBM_MS

                      val ((ariLBM_MS, riLBM_MS, nmiLBM_MS, nClusterLBM_MS), runtimeLBM_MS) = {
                        var t0 = System.nanoTime()
                        val bestLBM = FunLBM.ModelSelection.gridSearch(
                          dataMatrix,
                          rangeRow = List(4, 5, 6),
                          rangeCol = List(4, 5, 6),
                          verbose = verbose,
                          nConcurrentEachTest = 1,
                          nTryMaxPerConcurrent = 5)
                        val (rowMembershipLBM, colMembershipLBM) = (
                          bestLBM("RowPartition").asInstanceOf[List[Int]],
                          bestLBM("ColPartition").asInstanceOf[List[Int]])
                        val t1 = printTime(t0, "LBM MS")

                        val blockPartition = getBlockPartition(rowMembershipLBM, colMembershipLBM)
                        (getScores(blockPartition, trueBlockPartition), (t1 - t0)/1e9D)
                      }

                      ////////////////////////////////// BDPMM

                      val ((ariBDPMM, riBDPMM, nmiBDPMM, nClusterBDPMM), runtimeBDPMM) = {
                        var t0 = System.nanoTime()

                        val (rowMembershipsBDPMM, _, _) = new DAVID.FunNPLBM.CollapsedGibbsSampler(
                          dataList,
                          alphaPrior = alphaPrior,
                          betaPrior = betaPrior,
                          initByUserColPartition = Some(dataList.indices.toList)).runWithFixedPartitions(
                          nIter,
                          verbose = verbose, updateCol = false, updateRow = true)

                        val (_, colMembershipsBDPMM, _) = new DAVID.FunNPLBM.CollapsedGibbsSampler(
                          dataList,
                          alphaPrior = alphaPrior,
                          betaPrior = betaPrior,
                          initByUserRowPartition = Some(dataList.head.indices.toList)
                        ).runWithFixedPartitions(
                          nIter,
                          verbose = verbose, updateCol = true, updateRow = false)

                        val t1 = printTime(t0, "BDPMM")
                        val blockPartition = getBlockPartition(rowMembershipsBDPMM.last, colMembershipsBDPMM.last)
                        (getScores(blockPartition, trueBlockPartition), (t1 - t0)/1e9D)
                      }*/

                      //////////////////////////////////// NPLBM

                      /*val ((ariNPLBM, riNPLBM, nmiNPLBM, nClusterNPLBM), runtimeNPLBM) = {
                        var t0 = System.nanoTime()
                        val (rowMembershipNPLBM, colMembershipNPLBM, _) = new DAVID.FunNPLBM.CollapsedGibbsSampler(dataList,
                          alphaPrior = alphaPrior,
                          betaPrior = betaPrior).run(nIter, verbose = verbose)
                        val t1 = printTime(t0, "NPLBM")
                        val blockPartition = getBlockPartition(rowMembershipNPLBM.last, colMembershipNPLBM.last)
                        (getScores(blockPartition, trueBlockPartition), (t1 - t0)/1e9D)
                      }*/
                      //////////////////////////////////// Dis_NPLBM
                      val ((ariDis_NPLBM, riDis_NPLBM, nmiDis_NPLBM, nClusterDis_NPLBM), runtimeDis_NPLBM) ={
                        var t0 = System.nanoTime()
                        val (rowMembershipDis_NPLBM, colMembershipDis_NPLBM)= new DisNPLBM(master = sparkMaster,
                          dataRDD = workerRDD, alphaPrior = alphaPrior,
                          betaPrior = betaPrior,masterAlphaPrior = alhpa_master,workerAlphaPrior = alhpa_worker).run(nIter)
                        val t1 = printTime(t0, "Dis_NPLBM")
                        val blockPartition = getBlockPartition(rowMembershipDis_NPLBM, colMembershipDis_NPLBM)
                        (getScores(blockPartition, trueBlockPartition), (t1 - t0) / 1e9D)
                      }

                        println("ariDis_NPLBM=", ariDis_NPLBM)
                        println("riDis_NPLBM=", riDis_NPLBM)
                        println("nmiDis_NPLBM=", nmiDis_NPLBM)
                        println("nClusterDis_NPLBM=", nClusterDis_NPLBM)
                        println("runtimeDis_NPLBM=", runtimeDis_NPLBM)

                        /*println("ariNPLBM=",ariNPLBM)
                        println("riNPLBM=",riNPLBM)
                        println("nmiNPLBM=",nmiNPLBM)
                        println("nClusterNPLBM=",nClusterNPLBM)
                        println("runtimeNPLBM=",runtimeNPLBM)*/

                        /*val ARIs = Array(shape, scale, ariNPLBM, ariDis_NPLBM)
                        val RIs = Array(shape, scale, riNPLBM, riDis_NPLBM)
                        val NMIs = Array(shape, scale, nmiNPLBM, nmiDis_NPLBM)
                        val nClusters = Array(shape, scale, nClusterNPLBM, nClusterDis_NPLBM)
                        val runtimes = Array(shape, scale, runtimeNPLBM, runtimeDis_NPLBM)*/


                        /*val ARIs = Array(shape, scale, ariBGMM1, ariBGMM_MS, ariLBM1, ariLBM49, ariLBM_MS, ariBDPMM, ariNPLBM)
                        val RIs = Array(shape, scale, riBGMM1, riBGMM_MS, riLBM1, riLBM49, riLBM_MS, riBDPMM, riNPLBM)
                        val NMIs = Array(shape, scale, nmiBGMM1, nmiBGMM_MS, nmiLBM1, nmiLBM49, nmiLBM_MS, nmiBDPMM, nmiNPLBM)
                        val nClusters = Array(shape, scale, nClusterBGMM1, nClusterBGMM_MS, nClusterLBM1, nClusterLBM49, nClusterLBM_MS, nClusterBDPMM, nClusterNPLBM)
                        val runtimes = Array(shape, scale, runtimeBGMM1, runtimeBGMM_MS, runtimeLBM1, runtimeLBM49, runtimeLBM_MS, runtimeBDPMM, runtimeNPLBM)



                        val ARIMat = DenseMatrix(ARIs.map(_.toString)).reshape(1, ARIs.length)
                        val RIMat  = DenseMatrix( RIs.map(_.toString)).reshape(1, ARIs.length)
                        val NMIMat = DenseMatrix(NMIs.map(_.toString)).reshape(1, ARIs.length)
                        val nClusterMat = DenseMatrix(nClusters.map(_.toString)).reshape(1, ARIs.length)
                        val runtimesMat = DenseMatrix(runtimes.map(_.toString)).reshape(1, ARIs.length)

                        val append = true

                      IO.writeMatrixStringToCsv("src/main/scala/ARIs.csv", ARIMat, append=append)
                      IO.writeMatrixStringToCsv("src/main/scala/RIs.csv" , RIMat , append=append)
                      IO.writeMatrixStringToCsv("src/main/scala/NMIs.csv", NMIMat, append=append)
                      IO.writeMatrixStringToCsv("src/main/scala/nClusters.csv", nClusterMat, append=append)*/

                })
               /*val conf = new SparkConf().setMaster(sparkMaster).setAppName("DisNPLBM")

    val sc = new SparkContext(conf)

    // Parallelize and distribute data
    val dataByColRDD = sc.parallelize(dataList.zipWithIndex.map(e => (e._2, e._1)), 4 * 1)
    val dataByRowRDD = sc.parallelize((dataList.transpose).zipWithIndex.map(e => (e._2, e._1)), 4 * 1)
    val workerRowRDD = dataByRowRDD.mapPartitionsWithIndex((index, data) => {
      Iterator(new Line(index, data.toList))
    }).collect().toList
    println("sizeRow", workerRowRDD.size)
    val workerRDD = dataByColRDD.mapPartitionsWithIndex((index, data) => {
      val col = data.toList
      val row = workerRowRDD(index).my_data
      Iterator(new Plus(index, row, col))
    })
   val test= new DisNPLBM(master = sparkMaster,dataRDD = workerRDD, alphaPrior = alphaPrior,
     betaPrior = betaPrior)*/
        }
}

