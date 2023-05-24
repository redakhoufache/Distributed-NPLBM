package DAVID.FunDisNPLBM

import DAVID.Common.NormalInverseWishart
import DAVID.Common.Tools.partitionToOrderedCount
import breeze.linalg.DenseVector
import breeze.stats.distributions.Gamma
import org.apache.spark.rdd.RDD

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
                val dataRDD: RDD[DAVID.Plus],
                val master:String) extends Serializable {

  val workerRDD: RDD[WorkerNPLBM] = dataRDD.map(e => {
    new WorkerNPLBM(data = e, prior = prior, actualAlpha = actualAlpha, actualBeta = actualBeta)
  }).persist
  val dataByCol: List[List[DenseVector[Double]]]=dataRDD.map(e=>{
    e.my_data._2
  }).reduce(_ ++ _).sortBy(_._1).map(_._2)
  private val N: Int = dataByCol.head.length
  private val P: Int = dataByCol.length

  var prior: NormalInverseWishart = initByUserPrior match {
    case Some(pr) => pr
    case None => new NormalInverseWishart(dataByCol)
  }

  val d: Int = prior.d
  require(prior.d == dataByCol.head.head.length, "Prior and data dimensions differ")

  var rowPartition: List[Int] = initByUserRowPartition match {
    case Some(m) =>
      require(m.length == dataByCol.head.length)
      m
    case None => List.fill(N)(0)
  }

  var colPartition: List[Int] = initByUserColPartition match {
    case Some(m) =>
      require(m.length == dataByCol.length)
      m
    case None => List.fill(P)(0)
  }

  var countRowCluster: ListBuffer[Int] = partitionToOrderedCount(rowPartition).to[ListBuffer]
  var countColCluster: ListBuffer[Int] = partitionToOrderedCount(colPartition).to[ListBuffer]
  var NIWParamsByCol: ListBuffer[ListBuffer[NormalInverseWishart]] = (dataByCol zip colPartition).groupBy(_._2).values.map(e => {
    val dataPerColCluster = e.map(_._1).transpose
    val l = e.head._2
    (l, (dataPerColCluster zip rowPartition).groupBy(_._2).values.map(f => {
      val dataPerBlock = f.map(_._1).reduce(_ ++ _)
      val k = f.head._2
      (k, prior.update(dataPerBlock))
    }).toList.sortBy(_._1).map(_._2).to[ListBuffer])
  }).toList.sortBy(_._1).map(_._2).to[ListBuffer]

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

  def checkAlphaPrior(alpha: Option[Double], alphaPrior: Option[Gamma]): Boolean = {
    require(!(alpha.isEmpty & alphaPrior.isEmpty),
      "Either alphaRow or alphaRowPrior must be provided: please provide one of the two parameters.")
    require(!(alpha.isDefined & alphaPrior.isDefined),
      "Providing both alphaRow or alphaRowPrior is not supported: remove one of the two parameters.")
    alphaPrior.isDefined
  }
  def run(maxIter:Int,maxIterWorker:Int=1,maxIterMaster:Int=1): (List[Int],List[Int]) = {
    //Run dpm for row in each worker

    val workerNPLBM_result_row=workerRDD.map(worker=>{
      worker.runRow(maxIt=maxIterWorker,
        colPartition=colPartition,
      global_NIWParamsByCol=NIWParamsByCol)
    }).collect().toList.sortBy(_._1)
    workerNPLBM_result_row.foreach(e=>{
      println(e._1,e._3.size,(e._4.head.size,e._4.size))
    })
    println("-----------------------------------------------------------------")
    val row_master_result = new MasterNPLBM(actualAlpha = masterAlphaPrior, prior = prior,N = N,P= P).runRow(
      nIter = maxIterMaster,
      partitionOtherDimension = colPartition,
      workerResultsCompact = workerNPLBM_result_row,verbose = true
    )
    rowPartition = row_master_result._1
    NIWParamsByCol = row_master_result._2
    var local_row_partitions = row_master_result._3
    val workerNPLBM_result_col = workerRDD.map(worker => {
      worker.runCol(maxIt = maxIterWorker,
        rowPartition = rowPartition,
        global_NIWParamsByCol = NIWParamsByCol)
    }).collect().toList.sortBy(_._1)
    val col_master_result = new MasterNPLBM(actualAlpha = masterAlphaPrior, prior = prior,N = N,P= P).runCol(
      nIter = maxIterMaster,
      partitionOtherDimension = rowPartition,
      workerResultsCompact = workerNPLBM_result_col,verbose = true
    )
    colPartition = col_master_result._1
    NIWParamsByCol = col_master_result._2
    var local_col_partitions = col_master_result._3
    var it=2
    while (it<maxIter){
      val workerNPLBM_result_row = workerRDD.map(worker => {
        worker.runRow(maxIt = maxIterWorker,
          colPartition = colPartition,
          global_NIWParamsByCol = NIWParamsByCol,
          local_rowPartition = Some(local_row_partitions(worker.id)))
      }).collect().toList.sortBy(_._1)
      val row_master_result = new MasterNPLBM(actualAlpha = masterAlphaPrior, prior = prior,N = N,P= P).runRow(
        nIter = maxIterMaster,
        partitionOtherDimension = colPartition,
        workerResultsCompact = workerNPLBM_result_row,verbose = true
      )
      rowPartition = row_master_result._1
      NIWParamsByCol = row_master_result._2
      local_row_partitions = row_master_result._3
      val workerNPLBM_result_col = workerRDD.map(worker => {
        worker.runCol(maxIt = maxIterWorker,
          rowPartition = rowPartition,
          global_NIWParamsByCol = NIWParamsByCol,
          local_colPartition = Some(local_col_partitions(worker.id)))
      }).collect().toList.sortBy(_._1)
      val col_master_result = new MasterNPLBM(actualAlpha = masterAlphaPrior, prior = prior,N = N,P= P).runCol(
        nIter = maxIterMaster,
        partitionOtherDimension = rowPartition,
        workerResultsCompact = workerNPLBM_result_col,verbose = true
      )
      colPartition = col_master_result._1
      NIWParamsByCol = col_master_result._2
      local_col_partitions = col_master_result._3
      it=it+1
    }
    (rowPartition,colPartition)

  }
}
