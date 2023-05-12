
import Common.IO
import Common.Tools._
import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.Gamma
import org.apache.spark.sql.SparkSession

object Main {

  implicit val ss: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("AnalysePlan")
    .config("spark.executor.cores", 2)
    //.config("spark.executor.memory", "30G")
    .config("spark.executor.heartbeatInterval", "20s")
    .config("spark.driver.memory", "10G")
    .getOrCreate()

  ss.sparkContext.setLogLevel("WARN")
  ss.sparkContext.setCheckpointDir("checkpointDir")

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

                val nIter = 5
                val verbose = false
                val nLaunches = 10


                println("Benchmark begins. It is composed of "+ nLaunches.toString + " launches, each launch runs every methods once.")

                (0 until nLaunches).foreach(iter => {

                        var t0 = System.nanoTime()

                        println("Launch number " + iter)

                      //////////////////////////////////// BGMM_1

                      val ((ariBGMM1, riBGMM1, nmiBGMM1, nClusterBGMM1), runtimeBGMM1) = {

                        var t0 = System.nanoTime()

                        val bestBGMMRow = FunLBM.ModelSelection.gridSearch(
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

                        val (rowMembershipsBDPMM, _, _) = new FunNPLBM.CollapsedGibbsSampler(
                          dataList,
                          alphaPrior = alphaPrior,
                          betaPrior = betaPrior,
                          initByUserColPartition = Some(dataList.indices.toList)).runWithFixedPartitions(
                          nIter,
                          verbose = verbose, updateCol = false, updateRow = true)

                        val (_, colMembershipsBDPMM, _) = new FunNPLBM.CollapsedGibbsSampler(
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
                      }

                      //////////////////////////////////// NPLBM

                      val ((ariNPLBM, riNPLBM, nmiNPLBM, nClusterNPLBM), runtimeNPLBM) = {
                        var t0 = System.nanoTime()
                        val (rowMembershipNPLBM, colMembershipNPLBM, _) = new FunNPLBM.CollapsedGibbsSampler(dataList,
                          alphaPrior = alphaPrior,
                          betaPrior = betaPrior).run(nIter, verbose = verbose)
                        val t1 = printTime(t0, "NPLBM")
                        val blockPartition = getBlockPartition(rowMembershipNPLBM.last, colMembershipNPLBM.last)
                        (getScores(blockPartition, trueBlockPartition), (t1 - t0)/1e9D)
                      }

                        val ARIs = Array(shape, scale, ariBGMM1, ariBGMM_MS, ariLBM1, ariLBM49, ariLBM_MS, ariBDPMM, ariNPLBM)
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
                      IO.writeMatrixStringToCsv("src/main/scala/nClusters.csv", nClusterMat, append=append)

                })
        }
}

