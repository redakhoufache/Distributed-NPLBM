package Coclustering.DisNPLBM
import Coclustering.Common.NormalInverseWishart
import Coclustering.Common.Tools.partitionToOrderedCount
import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import breeze.linalg.{DenseMatrix, sum}

class DisNPLBM(var alpha: Double,
               var beta: Double,
               var initByUserPrior: Option[NormalInverseWishart] = None,
               var initByUserRowPartition: Option[List[Int]] = None,
               var initByUserColPartition: Option[List[Int]] = None,
               val dataRDD: RDD[Coclustering.Line]) extends Serializable {

  var actualAlpha: Double = alpha
  var actualBeta: Double = beta

  def computeSufficientStatistics(data: List[DenseVector[Double]]): (DenseVector[Double], DenseMatrix[Double], Int) = {
    val n = data.length.toDouble
    val meanData = data.reduce(_ + _) / n
    val covariance = sum(data.map(x => (x - meanData) * (x - meanData).t))
    (meanData, covariance, n.toInt)
  }

  private val N: Int = dataRDD.map(_.Data.size).reduce(_ + _)
  private val P: Int = dataRDD.first().Data.head._2.size

  def aggregateMeans(ms: List[DenseVector[Double]], ws: List[Int]): DenseVector[Double] = {
    val sums = (ms zip ws).par.map(e => e._2.toDouble * e._1).reduce(_ + _) / sum(ws).toDouble
    sums
  }

  def aggregateSquaredSums(sS: List[DenseMatrix[Double]],
                           ms: List[DenseVector[Double]],
                           ws: List[Int],
                           aggMean: DenseVector[Double]): DenseMatrix[Double] = {
    val aggM: DenseMatrix[Double] =
      sum(sS) + (ms zip ws).par.map(e => (e._1 * e._1.t) * e._2.toDouble).reduce(_ + _) - sum(ws).toDouble * aggMean * aggMean.t
    aggM
  }

  val ss: List[(DenseVector[Double], DenseMatrix[Double], Int)] = dataRDD.map(e => {
    computeSufficientStatistics(e.Data.flatMap(_._2))
  }).collect.toList
  val weights: List[Int] = ss.map(e => e._3)
  val sumWeights: Int = weights.sum
  val means: List[DenseVector[Double]] = ss.map(e => e._1)
  val squaredSums: List[DenseMatrix[Double]] = ss.map(e => e._2)
  val aggregatedMeans: DenseVector[Double] = aggregateMeans(means, weights)
  val aggregatedsS: DenseMatrix[Double] = aggregateSquaredSums(
    sS = squaredSums,
    ms = means,
    ws = weights,
    aggMean = aggregatedMeans
  )
  var prior: NormalInverseWishart = initByUserPrior match {
    case Some(pr) => pr
    case None => new NormalInverseWishart(globalVariance = aggregatedsS, globalMean = aggregatedMeans)
  }

  val d: Int = prior.d
  require(prior.d == aggregatedMeans.length, "Prior and data dimensions differ")

  var rowPartition: List[Int] = initByUserRowPartition match {
    case Some(m) =>
      require(m.length == N)
      m
    case None => List.fill(N)(0)
  }

  var colPartition: List[Int] = initByUserColPartition match {
    case Some(m) =>
      require(m.length == P)
      m
    case None => List.fill(P)(0)
  }

  var countRowCluster: ListBuffer[Int] = partitionToOrderedCount(rowPartition).to[ListBuffer]
  var countColCluster: ListBuffer[Int] = partitionToOrderedCount(colPartition).to[ListBuffer]
  var NIWParamsByCol: ListBuffer[ListBuffer[NormalInverseWishart]] =ListBuffer(ListBuffer(prior.updateFromSufficientStatistics(weight =sumWeights ,mean = aggregatedMeans,SquaredSum = aggregatedsS)))


  val workerRDD: RDD[WorkerNPLBM] = dataRDD.map(e => {
    new WorkerNPLBM(data = e, prior = prior, actualAlpha = actualAlpha, actualBeta = actualBeta,N = N)
  }).persist
  def run(maxIter:Int,maxIterWorker:Int=1): (List[Int],List[Int]) = {

    //Run dpm for row in each worker
    var t0 = System.nanoTime()
    val row_master_result=workerRDD.map(worker=>{
      worker.runRow(maxIt=maxIterWorker,
        colPartition=colPartition,
        globalNIWParamsByCol=NIWParamsByCol)
    }).reduce((x, y) => {
      x.runRow(partitionOtherDimension = colPartition, y)
    } )

    rowPartition = row_master_result.result._1
    NIWParamsByCol = row_master_result.result._2
    var local_row_partitions = row_master_result.result._3
    val result = row_master_result.runCol(
      rowPartition = rowPartition,
      globalNIW = NIWParamsByCol,
      colPartition = colPartition,
      mapLocalPartitionWithglobalPartition = row_master_result.mapClusterPartition
    )
    colPartition = result._2
    NIWParamsByCol = result._3

    var it=2
    while (it<maxIter){
      t0 = System.nanoTime()
      val row_master_result  = workerRDD.map(worker => {
        worker.runRow(maxIt = maxIterWorker,
          colPartition = colPartition,
          globalNIWParamsByCol = NIWParamsByCol,
          localRowPartition = Some(local_row_partitions(worker.id)))
      }).reduce((x, y) => {
        x.runRow( partitionOtherDimension = colPartition, y)
      })

      rowPartition = row_master_result.result._1
      NIWParamsByCol = row_master_result.result._2
      local_row_partitions = row_master_result.result._3
      val result =row_master_result.runCol(
        rowPartition = rowPartition,
        globalNIW = NIWParamsByCol,
        colPartition = colPartition,
        mapLocalPartitionWithglobalPartition = row_master_result.mapClusterPartition
      )
      colPartition=result._2
      /*rowPartition=result._1*/
      NIWParamsByCol=result._3
      t0 = System.nanoTime()
      System.out.println("it=",it)
      it=it+1

    }
    workerRDD.unpersist()
    (rowPartition,colPartition)
  }
}