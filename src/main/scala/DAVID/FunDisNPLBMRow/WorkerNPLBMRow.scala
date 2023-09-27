package DAVID.FunDisNPLBMRow

import DAVID.Common.NormalInverseWishart
import DAVID.Common.ProbabilisticTools.{normalizeLogProbability, sample}
import DAVID.Common.Tools.partitionToOrderedCount
import DAVID.FunDisNPLBM.aggregator
import breeze.linalg.{DenseMatrix, DenseVector, sum}
import breeze.numerics.log

import scala.collection.immutable.List
import scala.collection.mutable.ListBuffer

class WorkerNPLBMRow(
                   val data:DAVID.Line,
                   var prior: NormalInverseWishart,
                   val actualAlpha: Double,
                   val actualBeta: Double ,
                   var colPartition:List[Int],
                   val N:Int) extends  Serializable {
  val id:Int=data.id
  val n:Int=data.my_data.size
  val p:Int=data.my_data.head._2.size
  val row_indices=data.my_data.map(_._1)
  val DataByRow = data.my_data.map(_._2)
  val meanByRow=DataByRow.map(e=>{sum(e)/e.size.toDouble})
  val DataByRowT= DataByRow.transpose
  var NIWParamsByCol = (DataByRowT zip colPartition).groupBy(_._2).values.map(e => {
    val dataPerColCluster = e.map(_._1).transpose
    val l = e.head._2
    (l, (dataPerColCluster zip List.fill(n)(0)).groupBy(_._2).values.map(f => {
      val dataPerBlock = f.map(_._1).reduce(_ ++ _)
      val k = f.head._2
      (k, prior.update(dataPerBlock))
    }).toList.sortBy(_._1).map(_._2).to[ListBuffer])
  }).toList.sortBy(_._1).map(_._2).to[ListBuffer]
var countRowCluster = partitionToOrderedCount (List.fill (n) (0) ).to[ListBuffer]
  var local_row_partition=List.fill (n) (0)
  def priorPredictive(line: List[DenseVector[Double]],
                      partitionOtherDim: List[Int]): Double = {

    (line zip partitionOtherDim).groupBy(_._2).values.par.map(e => {
      val currentData = e.map(_._1)
      prior.jointPriorPredictive(currentData)
    }).toList.sum
  }

  def computeClusterMembershipProbabilities(x: List[DenseVector[Double]],
                                            partitionOtherDimension: List[Int],
                                            countCluster: ListBuffer[Int],
                                            NIWParams: ListBuffer[ListBuffer[NormalInverseWishart]],
                                            verbose: Boolean = false): List[Double] = {

    val xByRow = (x zip partitionOtherDimension).groupBy(_._2).map(v => (v._1, v._2.map(_._1)))
    NIWParams.indices.par.map(l => {
      (l, NIWParams.head.indices.par.map(k => {
        NIWParams(l)(k).jointPriorPredictive(xByRow(k))
      }).sum + log(countCluster(l)))
    }).toList.sortBy(_._1).map(_._2)
  }

  def drawMembership(x: List[DenseVector[Double]],
                     partitionOtherDimension: List[Int],
                     countCluster: ListBuffer[Int],
                     NIWParams: ListBuffer[ListBuffer[NormalInverseWishart]],
                     alpha: Double,
                     verbose: Boolean = false,index:Int): Int = {

    val probPartition = computeClusterMembershipProbabilities(x,
      partitionOtherDimension, countCluster, NIWParams, verbose)
    val posteriorPredictiveXi = priorPredictive(x, partitionOtherDimension)
    val probs = probPartition :+ (posteriorPredictiveXi + log(alpha))
    val normalizedProbs = normalizeLogProbability(probs)
    sample(normalizedProbs)
  }

  def computeLineSufficientStatistics(means: List[DenseVector[Double]]):
  (DenseVector[Double], DenseMatrix[Double], Int) = {
    val n = means.size.toDouble
    val meanData = sum(means)/n
    val covariance = sum(means.map(x => (x - meanData) * (x - meanData).t))
    (meanData, covariance, n.toInt)

  }
  def computeSufficientStatistics(data: List[DenseVector[Double]]): (DenseVector[Double], DenseMatrix[Double],Int) = {
    val n = data.length.toDouble
    val meanData = data.reduce(_ + _) / n
    val covariance = sum(data.map(x => (x - meanData) * (x - meanData).t))
    (meanData, covariance,n.toInt)
  }

  def computeBlockSufficientStatistics(data: List[List[DenseVector[Double]]],
                                       rowPartition: List[Int]):
  List[List[(DenseVector[Double], DenseMatrix[Double],Int)]] = {
    (data zip (0 until p).toList).groupBy(_._2).values.map(e => {
      val dataPerColCluster = e.map(_._1).transpose
      val l = e.head._2
      (l, (dataPerColCluster zip rowPartition).groupBy(_._2).values.map(f => {
        val dataPerBlock = f.map(_._1).reduce(_ ++ _)
        val k = f.head._2
        (k, computeSufficientStatistics(dataPerBlock))
      }).toList.sortBy(_._1).map(_._2))
    }).toList.sortBy(_._1).map(_._2)
  }
  def update_NPLBM_with_master_result(master_resutls:List[(Int, Int, ListBuffer[NormalInverseWishart], Int)],new_colPartition:List[Int]): Unit = {
    colPartition=new_colPartition
    val local_resut = master_resutls.filter(_._1 == this.id).sortBy(_._2)
    val tmp = local_resut.map(_._3)
    require(tmp.length==NIWParamsByCol.head.length,s"tmp.length=${tmp.length},  NIWParamsByCol.head.length=${NIWParamsByCol.head.length}")
    require(tmp.size == (local_row_partition.max + 1), s"tmp.size=${tmp.size} --- ${(local_row_partition.max + 1)}")
    NIWParamsByCol = (0 until colPartition.max + 1).indices.map(l => {
      (0 until tmp.size).indices.map(k => {
        tmp(k)(l)
      }).to[ListBuffer]
    }).to[ListBuffer]
    require(NIWParamsByCol.head.size == (local_row_partition.max + 1), s"NIWParamsByCol.size=${NIWParamsByCol.size} --- ${(local_row_partition.max + 1)}")
    require(NIWParamsByCol.size == (colPartition.max + 1), s"NIWParamsByCol.size=${NIWParamsByCol.size} --- ${colPartition.max + 1}")
  }

  def runRow(maxIt:Int,
          local_rowPartition: Option[List[Int]]=None,
             colPartition:List[Int],
          global_NIWParamsByCol:ListBuffer[ListBuffer[NormalInverseWishart]],
             master_resutls:Option[List[(Int, Int, ListBuffer[NormalInverseWishart], Int)]]=None
            ): aggregatorCol= {
    /*-----------------------------------------------Variables------------------------------------------------------*/
    /*var NIWParamsByCol: ListBuffer[ListBuffer[NormalInverseWishart]] =ListBuffer()*/
    var it=1
      /*-----------------------------------------------Row_partitioning----------------------------------------------*/
      /*-----------------------------------------------Variables-----------------------------------------------------*/

    /*var local_row_partition: List[Int] = local_rowPartition match {
        case Some(ro)=>{
          countRowCluster=partitionToOrderedCount(ro).to[ListBuffer]
          if (id==2)System.out.println(s"-2-->$ro")
          val tmp = ro.groupBy(identity).map(_._1).toList.sortBy(identity).sorted.zipWithIndex
          val tmp1 = ro.map(e => {
            tmp.filter(_._1 == e).head._2
          })
          val ro_number = NIWParamsByCol.head.size
          val deleted_ro = (0 until ro_number).diff(ro.distinct).reverse
          NIWParamsByCol.indices.foreach(i=>{
            val col=NIWParamsByCol(i)
                deleted_ro.foreach(j => col.remove(j))
          })
          tmp1
        }
        case None=> {
          countRowCluster=partitionToOrderedCount(List.fill(n)(0)).to[ListBuffer]
          List.fill(n)(0)
        }
      }*/
    if (id==2)System.out.println(s"0-->local_row_partition= $local_row_partition")
      /*----------------------------------------------Functions------------------------------------------------------*/
      def removeElementFromRowCluster(row: List[DenseVector[Double]], currentPartition: Int): Unit = {
        if (countRowCluster(currentPartition) == 1) {
          countRowCluster.remove(currentPartition)
          NIWParamsByCol.map(e => e.remove(currentPartition))
          local_row_partition = local_row_partition.map(c => {
            if (c > currentPartition) {
              c - 1
            } else c
          })
        } else {
          countRowCluster.update(currentPartition, countRowCluster.apply(currentPartition) - 1)
          (row zip colPartition).groupBy(_._2).values.foreach(e => {
            val l = e.head._2
            val dataInCol = e.map(_._1)
            NIWParamsByCol(l).update(currentPartition,
              NIWParamsByCol(l)(currentPartition).removeObservations(dataInCol))
          })
        }
      }

      def addElementToRowCluster(row: List[DenseVector[Double]],
                                 newPartition: Int): Unit = {

        if (newPartition == countRowCluster.length) {
          countRowCluster = countRowCluster ++ ListBuffer(1)
          (row zip colPartition).groupBy(_._2).values.foreach(e => {
            val l = e.head._2
            val dataInCol = e.map(_._1)
            val newNIWparam = this.prior.update(dataInCol)
            NIWParamsByCol(l) = NIWParamsByCol(l) ++ ListBuffer(newNIWparam)
          })
        } else {
          countRowCluster.update(newPartition, countRowCluster.apply(newPartition) + 1)
          (row zip colPartition).groupBy(_._2).values.foreach(e => {
            val l = e.head._2
            val dataInCol = e.map(_._1)
            NIWParamsByCol(l).update(newPartition,
              NIWParamsByCol(l)(newPartition).update(dataInCol))
          })
        }
      }

      def updateRowPartition(verbose: Boolean = false) = {

        for (i <- DataByRow.indices) {
          val currentData = DataByRow(i)
          val currentPartition = local_row_partition(i)
          removeElementFromRowCluster(currentData, currentPartition)
          val newPartition = drawMembership(currentData,
            colPartition, countRowCluster, NIWParamsByCol.transpose, actualAlpha,index = i)
          local_row_partition = local_row_partition.updated(i, newPartition)
          addElementToRowCluster(currentData, newPartition)
        }
      }

      while(it<=maxIt)
        {
          updateRowPartition()
          it=it+1
        }
        val row_sufficientStatistic=(meanByRow zip local_row_partition).groupBy(_._2).values.map(e=>{
          val dataPerRowCluster = e.map(_._1)
          (dataPerRowCluster,e.head._2)
        }).toList.map(e=>(computeLineSufficientStatistics(e._1),e._2))
    if (id==2)System.out.println(s"1-->local_row_partition= $local_row_partition")
    new aggregatorCol(actualAlpha = actualAlpha,
      prior = prior,
      line_sufficientStatistic = row_sufficientStatistic,
      map_partition = (local_row_partition zip row_indices).map(e => {
        (this.id, e._1, e._2)
      }).to[ListBuffer],
      blockss = ListBuffer((this.id, computeBlockSufficientStatistics(DataByRowT, local_row_partition))),
      N = N, worker_id = this.id,beta=actualBeta).run()

    }
}
