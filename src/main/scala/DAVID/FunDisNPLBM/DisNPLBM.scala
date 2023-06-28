package DAVID.FunDisNPLBM

import DAVID.Common.NormalInverseWishart
import DAVID.Common.Tools.{partitionToOrderedCount, printTime}
import breeze.linalg.DenseVector
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
    //Run dpm for row in each worker
    var t0 = System.nanoTime()
    val workerNPLBM_result_row=workerRDD.map(worker=>{
      worker.runRow(maxIt=maxIterWorker,
        colPartition=colPartition,
      global_NIWParamsByCol=NIWParamsByCol)
    }).collect().par/*.reduce((x, y) => x ++ y).sortBy(_._1)*/
    var t1 = printTime(t0, s"setp 1 worker row")
    System.out.println("setp 1 worker row",(t1 - t0) / 1e9D)
    t0 = System.nanoTime()
    /*val row_master_result = new MasterNPLBM(actualAlpha = actualAlpha, prior = prior,N = N,P= P).runRow(
      nIter = maxIterMaster,
      partitionOtherDimension = colPartition,
      workerResultsCompact = workerNPLBM_result_row.toList
    )*/
    val row_master_result =workerNPLBM_result_row.reduce((x, y) => x.runRow(nIter = 1,partitionOtherDimension = colPartition,y)).result
    t1 = printTime(t0, s"setp 1 master row")
    System.out.println("setp 1 master row", (t1 - t0) / 1e9D)
    rowPartition = row_master_result._1
    NIWParamsByCol = row_master_result._2
    var local_row_partitions = row_master_result._3
    t0 = System.nanoTime()
    val workerNPLBM_result_col = workerRDD.map(worker => {

      worker.runCol(maxIt = maxIterWorker,
        rowPartition = rowPartition,
        global_NIWParamsByCol = NIWParamsByCol)
    })
    t1 = printTime(t0, s"setp 1 worker col")
    System.out.println("setp 1 worker col",(t1 - t0) / 1e9D)
    t0 = System.nanoTime()
    /*val col_master_result = new MasterNPLBM(actualAlpha = actualAlpha, prior = prior,N = N,P= P).runCol(
      nIter = maxIterMaster,
      partitionOtherDimension = rowPartition,
      workerResultsCompact = workerNPLBM_result_col.toList
    )*/
    val col_master_result1=workerNPLBM_result_col
    val col_master_result=col_master_result1.collect().par.reduce((x, y) => x.runCol(nIter = 1,partitionOtherDimension = rowPartition,y)).result
    t1 = printTime(t0, s"setp 1 master col")
    System.out.println("setp 1 master col", (t1 - t0) / 1e9D)
    colPartition = col_master_result._1
    NIWParamsByCol = col_master_result._2
    var local_col_partitions = col_master_result._3
    println("colPartition=",colPartition)
    println("local_col_partitions=",local_col_partitions)
    var it=2
    while (it<maxIter){
      t0 = System.nanoTime()
      val workerNPLBM_result_row = workerRDD.map(worker => {
        worker.runRow(maxIt = maxIterWorker,
          colPartition = colPartition,
          global_NIWParamsByCol = NIWParamsByCol,
          local_rowPartition = Some(local_row_partitions(worker.id)))
      }).collect().par/*.reduce((x, y) => x ++ y).sortBy(_._1)*/
      t1 = printTime(t0, s"setp $it worker row")
      System.out.println(s"setp $it worker row", (t1 - t0) / 1e9D)
      t0 = System.nanoTime()
      /*val row_master_result = new MasterNPLBM(actualAlpha = actualAlpha, prior = prior,N = N,P= P).runRow(
        nIter = maxIterMaster,
        partitionOtherDimension = colPartition,
        workerResultsCompact = workerNPLBM_result_row
      )*/
      val row_master_result =workerNPLBM_result_row.reduce((x, y) => x.runRow(nIter = 1,partitionOtherDimension = colPartition,y)).result
      t1 = printTime(t0, s"setp $it master row")
      System.out.println(s"setp $it master row", (t1 - t0) / 1e9D)

      rowPartition = row_master_result._1
      NIWParamsByCol = row_master_result._2
      local_row_partitions = row_master_result._3
      t0 = System.nanoTime()
      val workerNPLBM_result_col = workerRDD.map(worker => {worker.runCol(maxIt = maxIterWorker,
        rowPartition = rowPartition,
        global_NIWParamsByCol = NIWParamsByCol,
        local_colPartition = Some(local_col_partitions(worker.id)))
      }).collect().par
      t1 = printTime(t0, s"setp $it worker col")
      System.out.println(s"setp $it worker col", (t1 - t0) / 1e9D)


      t0 = System.nanoTime()
      val col_master_result =workerNPLBM_result_col.reduce((x, y) => x.runCol(nIter = 1,partitionOtherDimension = rowPartition,y)).result
      t1 = printTime(t0, s"setp $it master col")
      System.out.println(s"setp $it master col", (t1 - t0) / 1e9D)
      /*t0 = System.nanoTime()
      val col_master_result = new MasterNPLBM(actualAlpha = actualAlpha, prior = prior,N = N,P= P).runCol(
        nIter = maxIterMaster,
        partitionOtherDimension = rowPartition,
        workerResultsCompact = workerNPLBM_result_col
      )
      t1 = printTime(t0, s"setp $it master col")
      System.out.println(s"setp $it master col", (t1 - t0) / 1e9D)*/
      colPartition = col_master_result._1
      NIWParamsByCol = col_master_result._2
      local_col_partitions = col_master_result._3
      it=it+1
    }
    (rowPartition,colPartition)

  }
}
