package Coclustering.DisNPLBM

import Coclustering.Common.NormalInverseWishart
import Coclustering.Common.ProbabilisticTools.{normalizeLogProbability, sample}
import Coclustering.Common.Tools.partitionToOrderedCount
import breeze.linalg.{DenseMatrix, DenseVector, sum}
import breeze.numerics.log

import scala.collection.immutable.SortedMap
import scala.collection.mutable.ListBuffer

class WorkerNPLBM(val data: Coclustering.Line,
                  var prior: NormalInverseWishart,
                  val actualAlpha: Double,
                  val actualBeta: Double,
                  val N: Int) extends  Serializable {

  val id: Int = data.ID
  val n: Int = data.Data.size
  val p: Int = data.Data.head._2.size
  val rowIndices = data.Data.map(_._1)
  val DataByRow = data.Data.map(_._2)
  val meanByRow = DataByRow.map(e=>{sum(e)/e.size.toDouble})
  val DataByRowT = DataByRow.transpose

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

  def runRow(maxIt:Int,
             localRowPartition: Option[List[Int]]=None,
             colPartition:List[Int],
             globalNIWParamsByCol:ListBuffer[ListBuffer[NormalInverseWishart]]
            ): Aggregator= {

    var NIWParamsByCol: ListBuffer[ListBuffer[NormalInverseWishart]] = globalNIWParamsByCol
    var it=1
    var countRowCluster: ListBuffer[Int] =ListBuffer()
    var localRowMembership: List[Int] = localRowPartition match {
      case Some(ro)=>{
        countRowCluster=partitionToOrderedCount(ro).to[ListBuffer]
        val tmp = ro.groupBy(identity).map(_._1).toList.sortBy(identity).sorted.zipWithIndex
        val tmp1 = ro.map(e => {
          tmp.filter(_._1 == e).head._2
        })
        val roNumber = NIWParamsByCol.head.size
        val deletedRo = (0 until roNumber).diff(ro.distinct).reverse
        NIWParamsByCol.indices.foreach(i=>{
          val col=NIWParamsByCol(i)
          deletedRo.foreach(j => col.remove(j))
        })
        require(NIWParamsByCol.head.length==(tmp1.max+1),
          s"worker_id=${this.id}  ${NIWParamsByCol.head.size}==${tmp1.max+1}")
        /* System.out.println(s"worker_id=${this.id}-->${ro zip tmp1}")*/
        tmp1
      }
      case None=> {
        countRowCluster=partitionToOrderedCount(List.fill(n)(0)).to[ListBuffer]
        List.fill(n)(0)
      }
    }
    /*----------------------------------------------Functions------------------------------------------------------*/
    def removeElementFromRowCluster(row: List[DenseVector[Double]], currentPartition: Int): Unit = {
      if (countRowCluster(currentPartition) == 1) {
        countRowCluster.remove(currentPartition)
        NIWParamsByCol.map(e => e.remove(currentPartition))
        localRowMembership = localRowMembership.map(c => {
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
        val currentPartition = localRowMembership(i)
        removeElementFromRowCluster(currentData, currentPartition)
        val newPartition = drawMembership(currentData,
          colPartition, countRowCluster, NIWParamsByCol.transpose, actualAlpha,index = i)
        localRowMembership = localRowMembership.updated(i, newPartition)
        addElementToRowCluster(currentData, newPartition)
      }
    }

    while(it<=maxIt)
    {
      updateRowPartition()
      it=it+1
    }
    val row_sufficientStatistic=SortedMap((meanByRow zip localRowMembership).groupBy(_._2).toSeq: _*).values.map(e=>{
      val dataPerRowCluster = e.map(_._1)
      (dataPerRowCluster,e.head._2)
    }).toList.map(e=>(computeLineSufficientStatistics(e._1),e._2))
    new Aggregator(actualAlpha = actualAlpha,
      prior = prior,
      sufficientStatisticByRow = row_sufficientStatistic,
      mapPartition = (localRowMembership zip rowIndices).map(e => {
        (this.id, e._1, e._2)
      }).to[ListBuffer],
      sufficientStatisticsByBlock = ListBuffer((this.id, computeBlockSufficientStatistics(DataByRowT, localRowMembership))),
      N = N, workerId = this.id,beta=actualBeta).run()

  }
}