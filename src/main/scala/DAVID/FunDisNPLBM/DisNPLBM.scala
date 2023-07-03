package DAVID.FunDisNPLBM

import DAVID.Common.NormalInverseWishart
import DAVID.Common.Tools.{partitionToOrderedCount, printTime}
import breeze.linalg.DenseVector
import breeze.numerics.log10
import org.apache.spark.rdd.RDD
import shapeless.syntax.std.tuple.productTupleOps

import scala.collection.mutable.ListBuffer

class DisNPLBM (val masterAlphaPrior: Double=5.0,
                val workerAlphaPrior: Double=5.0,
                var actualAlpha : Double=5.0,
                var actualBeta: Double=5.0,
                var initByUserPrior: Option[NormalInverseWishart] = None,
                var initByUserRowPartition: Option[List[Int]] = None,
                var initByUserColPartition: Option[List[Int]] = None,
                val dataRDD: RDD[DAVID.Plus],
                val master:String) extends Serializable {


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
  var NIWParamsByCol: ListBuffer[ListBuffer[NormalInverseWishart]] = (dataByCol zip colPartition).groupBy(_._2)
    .values.map(e => {
    val dataPerColCluster = e.map(_._1).transpose
    val l = e.head._2
    (l, (dataPerColCluster zip rowPartition).groupBy(_._2).values.map(f => {
      val dataPerBlock = f.map(_._1).reduce(_ ++ _)
      val k = f.head._2
      (k, prior.update(dataPerBlock))
    }).toList.sortBy(_._1).map(_._2).to[ListBuffer])
  }).toList.sortBy(_._1).map(_._2).to[ListBuffer]


  val workerRDD: RDD[WorkerNPLBM] = dataRDD.map(e => {
    new WorkerNPLBM(data = e, prior = prior, actualAlpha = actualAlpha, actualBeta = actualBeta)
  }).persist
  def run(maxIter:Int,maxIterWorker:Int=1,maxIterMaster:Int=1): (List[Int],List[Int]) = {
    val numParation=workerRDD.getNumPartitions
    val depth=(log10(numParation)/log10(2.0)).toInt
    //Run dpm for row in each worker
    var t0 = System.nanoTime()
    val row_master_result=workerRDD.map(worker=>{
      worker.runRow(maxIt=maxIterWorker,
        colPartition=colPartition,
      global_NIWParamsByCol=NIWParamsByCol.clone())
    }).reduce((x, y) => {
       x.runRow(partitionOtherDimension = colPartition, y)
    } ).result

    rowPartition = row_master_result._1
    NIWParamsByCol = row_master_result._2
    var local_row_partitions = row_master_result._3

    val col_master_result = workerRDD.map(worker => {

      worker.runCol(maxIt = maxIterWorker,
        rowPartition = rowPartition,
        global_NIWParamsByCol = NIWParamsByCol.clone())
    }).reduce((x, y) => {
      x.runCol( partitionOtherDimension = rowPartition, y)
    }).result

    colPartition = col_master_result._1
    NIWParamsByCol = col_master_result._2
    var local_col_partitions = col_master_result._3

    var it=2
    while (it<maxIter){
      t0 = System.nanoTime()
      val row_master_result  = workerRDD.map(worker => {
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
      val col_master_result = workerRDD.map(worker => {worker.runCol(maxIt = maxIterWorker,
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
