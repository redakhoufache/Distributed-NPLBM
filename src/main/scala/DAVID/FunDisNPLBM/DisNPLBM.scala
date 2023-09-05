package DAVID.FunDisNPLBM

import DAVID.Common.NormalInverseWishart
import DAVID.Common.Tools.{partitionToOrderedCount, printTime}
import DAVID.Line
import breeze.linalg.{DenseMatrix, DenseVector, sum}
import breeze.numerics.log10
import org.apache.spark.rdd.RDD
import breeze.stats.distributions.Gamma
import shapeless.syntax.std.tuple.productTupleOps

import scala.collection.mutable.ListBuffer

class DisNPLBM (val masterAlphaPrior: Double=5.0,
                val workerAlphaPrior: Double=5.0,
                var alpha: Option[Double] = None,
                var beta: Option[Double] = None,
                var alphaPrior: Option[Gamma] = None,
                var betaPrior: Option[Gamma] = None,
                var initByUserPrior: Option[NormalInverseWishart] = None,
                var initByUserRowPartition: Option[List[Int]] = None,
                var initByUserColPartition: Option[List[Int]] = None,
                val dataRDDrow: RDD[DAVID.Line],
                val dataRDDcol: RDD[DAVID.Line],
                val master:String) extends Serializable {
  private val N: Int = dataRDDcol.first().my_data.head._2.size
  private val P: Int = dataRDDrow.first().my_data.head._2.size

  def computeSufficientStatistics(data: List[DenseVector[Double]]): (DenseVector[Double], DenseMatrix[Double], Int) = {
    val n = data.length.toDouble
    val meanData = data.reduce(_ + _) / n
    val covariance = sum(data.map(x => (x - meanData) * (x - meanData).t))
    (meanData, covariance, n.toInt)
  }
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

  def checkAlphaPrior(alpha: Option[Double], alphaPrior: Option[Gamma]): Boolean = {
    require(!(alpha.isEmpty & alphaPrior.isEmpty), "Either alphaRow or alphaRowPrior must be provided: please provide one of the two parameters.")
    require(!(alpha.isDefined & alphaPrior.isDefined), "Providing both alphaRow or alphaRowPrior is not supported: remove one of the two parameters.")
    alphaPrior.isDefined
  }
  var updateAlphaFlag: Boolean = checkAlphaPrior(alpha, alphaPrior)
  var updateBetaFlag: Boolean = checkAlphaPrior(beta, betaPrior)

  var actualAlphaPrior: Gamma = alphaPrior match {
    case Some(g) => g
    case None => new Gamma(1D, 1D)
  }
  var actualBetaPrior: Gamma = betaPrior match {
    case Some(g) => g
    case None => new Gamma(1D, 1D)
  }

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
  val ss = dataRDDrow.map(e => {
    computeSufficientStatistics(e.my_data.flatMap(_._2))
  }).collect.toList
  val weights: List[Int] = ss.map(e => e._3)
  val sum_weights: Int = weights.sum
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
    case None => {
      new NormalInverseWishart(globalVariance = aggregatedsS, globalMean = aggregatedMeans)
    }
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
  var NIWParamsByCol: ListBuffer[ListBuffer[NormalInverseWishart]] = ListBuffer(ListBuffer(
    prior.updateFromSufficientStatistics(weight = sum_weights, mean = aggregatedMeans, SquaredSum = aggregatedsS)))

  val fontom=new Line(0,List.fill(1)((0,List.fill(1)(DenseVector(0)))))
  val workerRDDrow: RDD[WorkerNPLBM] = dataRDDrow.map(e => {
    new WorkerNPLBM(worker_id = e.id,datarow = e,datacol = fontom, prior = prior, actualAlpha = actualAlpha,
      actualBeta = actualBeta,
      N = N,
      P = P)
  }).persist
  val workerRDDcol: RDD[WorkerNPLBM] = dataRDDcol.map(e => {
    new WorkerNPLBM(worker_id = e.id,datarow = fontom,datacol = e, prior = prior, actualAlpha = actualAlpha, actualBeta = actualBeta,
      N = N,
      P = P)
  }).persist
  def run(maxIter:Int,maxIterWorker:Int=1,maxIterMaster:Int=1): (List[Int],List[Int]) = {
    val numParation=workerRDDrow.getNumPartitions
    val depth=(log10(numParation)/log10(2.0)).toInt
    //Run dpm for row in each worker
    var t0 = System.nanoTime()
    val row_master_result=workerRDDrow.map(worker=>{
      worker.runRow(maxIt=maxIterWorker,
        colPartition=colPartition,
      global_NIWParamsByCol=NIWParamsByCol.clone())
    }).reduce((x, y) => {
       x.runRow(partitionOtherDimension = colPartition, y)
    } ).result

    rowPartition = row_master_result._1
    NIWParamsByCol = row_master_result._2
    var local_row_partitions = row_master_result._3
    val col_master_result = workerRDDcol.map(worker => {
      worker.runCol(maxIt = maxIterWorker,
        rowPartition = rowPartition,
        global_NIWParamsByCol = NIWParamsByCol.clone())
    }).reduce((x, y) => {
      x.runCol(partitionOtherDimension = rowPartition, y)
    }).result

    colPartition = col_master_result._1
    NIWParamsByCol = col_master_result._2
    var local_col_partitions = col_master_result._3

    var it=2
    while (it<maxIter){
      t0 = System.nanoTime()
      val row_master_result  = workerRDDrow.map(worker => {
        worker.runRow(maxIt = maxIterWorker,
          colPartition = colPartition,
          global_NIWParamsByCol = NIWParamsByCol.clone(),
          local_rowPartition = Some(local_row_partitions(worker.id)))
      }).reduce((x, y) => {
        x.runRow( partitionOtherDimension = colPartition, y)
      }).result

      rowPartition = row_master_result._1
      NIWParamsByCol = row_master_result._2
      local_row_partitions = row_master_result._3
      t0 = System.nanoTime()
      val col_master_result = workerRDDcol.map(worker => {worker.runCol(maxIt = maxIterWorker,
        rowPartition = rowPartition,
        global_NIWParamsByCol = NIWParamsByCol.clone(),
        local_colPartition = Some(local_col_partitions(worker.id)))
      }).reduce((x, y) => {
        x.runCol( partitionOtherDimension = rowPartition, y)
      }).result

      colPartition = col_master_result._1
      NIWParamsByCol = col_master_result._2
      local_col_partitions = col_master_result._3
      System.out.println("it=",it)
      it=it+1
    }
    (rowPartition,colPartition)

  }
}
