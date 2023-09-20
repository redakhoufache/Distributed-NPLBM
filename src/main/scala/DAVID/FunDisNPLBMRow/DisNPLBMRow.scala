package DAVID.FunDisNPLBMRow

import DAVID.Common.NormalInverseWishart
import DAVID.Common.ProbabilisticTools.updateAlpha
import DAVID.Common.Tools.{getBlockPartition, getScores, partitionToOrderedCount}
import breeze.linalg.DenseVector
import breeze.stats.distributions.Gamma
import org.apache.spark.rdd.RDD
import breeze.numerics.log10

import scala.collection.mutable.ListBuffer
import breeze.linalg.{DenseMatrix, DenseVector, sum}
class DisNPLBMRow(
                   var alpha: Option[Double] = None,
                   var beta: Option[Double] = None,
                   var alphaPrior: Option[Gamma] = None,
                   var betaPrior: Option[Gamma] = None,
                   var initByUserPrior: Option[NormalInverseWishart] = None,
                   var initByUserRowPartition: Option[List[Int]] = None,
                   var initByUserColPartition: Option[List[Int]] = None,
                   val dataRDD: RDD[DAVID.Line],
                   val master:String,
                   val likelihood:Boolean=false,
                   val score:Boolean=false,
                   val alldata: List[List[DenseVector[Double]]]=List(List(DenseVector(0.0))),
                   val trueBlockPartition:List[Int]=List(0)
                 ) extends Serializable {
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

  def computeSufficientStatistics(data: List[DenseVector[Double]]): (DenseVector[Double], DenseMatrix[Double], Int) = {
    val n = data.length.toDouble
    val meanData = data.reduce(_ + _) / n
    val covariance = sum(data.map(x => (x - meanData) * (x - meanData).t))
    (meanData, covariance, n.toInt)
  }

  private val N: Int = dataRDD.map(_.my_data.size).reduce(_ + _)
  private val P: Int = dataRDD.first().my_data.head._2.size

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

  val ss = dataRDD.map(e => {
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
    case None =>{
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
  var NIWParamsByCol: ListBuffer[ListBuffer[NormalInverseWishart]] =ListBuffer(ListBuffer(
    prior.updateFromSufficientStatistics(weight =sum_weights ,mean = aggregatedMeans,SquaredSum = aggregatedsS)))


  val workerRDD: RDD[WorkerNPLBMRow] = dataRDD.map(e => {
    new WorkerNPLBMRow(data = e, prior = prior, actualAlpha = actualAlpha, actualBeta = actualBeta,N = N)
  }).persist
  def run(maxIter:Int,maxIterWorker:Int=1,maxIterMaster:Int=1): (List[Int],List[Int]) = {
    //Run dpm for row in each worker
    val numParation=workerRDD.getNumPartitions
    val depth=(log10(numParation)/log10(2.0)).toInt
    var t0 = System.nanoTime()
    val row_master_result=workerRDD.map(worker=>{
      worker.runRow(maxIt=maxIterWorker,
        colPartition=colPartition,
      global_NIWParamsByCol=NIWParamsByCol)
    }).reduce((x, y) => {
       x.runRow(partitionOtherDimension = colPartition, y)
    } )

    rowPartition = row_master_result.result._1
    NIWParamsByCol = row_master_result.result._2
    var local_row_partitions = row_master_result.result._3
    val result = row_master_result.runCol(
      row_partition = rowPartition,
      global_NIW = NIWParamsByCol,
      col_partition = colPartition,
      map_localPart_globalPart = row_master_result.map_cluster_Partition
    )
    colPartition = result._2
    rowPartition = result._1
    NIWParamsByCol = result._3
    if (updateAlphaFlag) actualAlpha = updateAlpha(actualAlpha, actualAlphaPrior, (rowPartition.max + 1), N)
    if (updateBetaFlag) actualBeta = updateAlpha(actualBeta, actualBetaPrior, (colPartition.max + 1), P)
    if(score){
      val (ari, ri, nmi, nCluster)=getScores(getBlockPartition(rowPartition,colPartition), trueBlockPartition)
      System.out.println("ari =", ari)
      System.out.println("ri =", ri)
      System.out.println("nmi =", nmi)
      System.out.println("nCluster =", nCluster)
    }
    if(likelihood) {
      val likelihood= prior.NPLBMlikelihood(
        alphaRow = actualAlpha,
        alphaCol = actualBeta,
        dataByCol = alldata,
        rowMembership = rowPartition,
        colMembership = colPartition,
        countRowCluster = partitionToOrderedCount(rowPartition),
        countColCluster = partitionToOrderedCount(colPartition),
        componentsByCol = NIWParamsByCol.map(_.map(_.sample()).toList).toList)
      System.out.println("likelihood =", likelihood)
    }
    var it=2
    while (it<maxIter){
      t0 = System.nanoTime()
      val row_master_result  = workerRDD.map(worker => {
        worker.runRow(maxIt = maxIterWorker,
          colPartition = colPartition,
          global_NIWParamsByCol = NIWParamsByCol,
          local_rowPartition = Some(local_row_partitions(worker.id)))
      }).reduce((x, y) => {
        x.runRow( partitionOtherDimension = colPartition, y)
      })

      rowPartition = row_master_result.result._1
      NIWParamsByCol = row_master_result.result._2
      local_row_partitions = row_master_result.result._3
      val result =row_master_result.runCol(
        row_partition = rowPartition,
        global_NIW = NIWParamsByCol,
        col_partition = colPartition,
        map_localPart_globalPart = row_master_result.map_cluster_Partition
      )
      colPartition=result._2
      rowPartition=result._1
      NIWParamsByCol=result._3
      t0 = System.nanoTime()
      System.out.println("it=",it)
      it=it+1
      if (updateAlphaFlag) actualAlpha = updateAlpha(actualAlpha, actualAlphaPrior, (rowPartition.max + 1), N)
      if (updateBetaFlag) actualBeta = updateAlpha(actualBeta, actualBetaPrior, (colPartition.max + 1), P)
      if (score) {
        val testBlockPartition=getBlockPartition(rowPartition, colPartition)
        val (ari, ri, nmi, nCluster) = getScores(testBlockPartition, trueBlockPartition)
        System.out.println("ari", ari)
        System.out.println("ri", ri)
        System.out.println("nmi", nmi)
        System.out.println("nCluster", nCluster)
      }
      if (likelihood) {
        val likelihood = prior.NPLBMlikelihood(
          alphaRow = actualAlpha,
          alphaCol = actualBeta,
          dataByCol = alldata,
          rowMembership = rowPartition,
          colMembership = colPartition,
          countRowCluster = partitionToOrderedCount(rowPartition),
          countColCluster = partitionToOrderedCount(colPartition),
          componentsByCol = NIWParamsByCol.map(_.map(_.sample()).toList).toList)
        System.out.println("likelihood =", likelihood)
      }
    }
    workerRDD.unpersist()
    (rowPartition,colPartition)

  }
}
