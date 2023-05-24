package DAVID.FunDisNPLBM

import DAVID.Common.NormalInverseWishart
import DAVID.Common.ProbabilisticTools.{normalizeLogProbability, sample}
import DAVID.Common.Tools.partitionToOrderedCount
import breeze.linalg.{DenseMatrix, DenseVector, sum}
import breeze.numerics.log

import scala.collection.mutable.ListBuffer

class WorkerNPLBM (
                   val data:DAVID.Plus,
                   var prior: NormalInverseWishart,
                   val actualAlpha: Double,
                   val actualBeta: Double ) extends  Serializable {
  val id:Int=data.id
  val n:Int=data.my_data._1.size
  val p:Int=data.my_data._2.size
  val col_indices=data.my_data._2.map(e => e._1)
  val DataByCol = data.my_data._2.map(e => e._2)
  val row_indices=data.my_data._1.map(e => e._1)
  val DataByRow = data.my_data._1.map(e => e._2)
  val DataByRowT= DataByRow.transpose
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
                     verbose: Boolean = false): Int = {

    val probPartition = computeClusterMembershipProbabilities(x,
      partitionOtherDimension, countCluster, NIWParams, verbose)
    val posteriorPredictiveXi = priorPredictive(x, partitionOtherDimension)
    val probs = probPartition :+ (posteriorPredictiveXi + log(alpha))
    val normalizedProbs = normalizeLogProbability(probs)
    sample(normalizedProbs)
  }

  def computeSufficientStatistics(data: List[DenseVector[Double]]): (DenseVector[Double], DenseMatrix[Double],Int) = {
    val n = data.length.toDouble
    val meanData = data.reduce(_ + _) / n.toDouble
    val covariance = sum(data.map(x => (x - meanData) * (x - meanData).t))
    (meanData, covariance,n.toInt)
  }

  def computeBlockSufficientStatistics(data: List[List[DenseVector[Double]]],
                                       colPartition: List[Int],
                                       rowPartition: List[Int]):
  List[List[(DenseVector[Double], DenseMatrix[Double],Int)]] = {
    (data zip colPartition).groupBy(_._2).values.map(e => {
      val dataPerColCluster = e.map(_._1).transpose
      val l = e.head._2
      (l, (dataPerColCluster zip rowPartition).groupBy(_._2).values.map(f => {
        val dataPerBlock = f.map(_._1).reduce(_ ++ _)
        val k = f.head._2
        (k, computeSufficientStatistics(dataPerBlock))
      }).toList.sortBy(_._1).map(_._2))
    }).toList.sortBy(_._1).map(_._2)
  }

  def runCol(maxIt: Int,
             rowPartition: List[Int],
          local_colPartition: Option[List[Int]] = None,
          global_NIWParamsByCol: ListBuffer[ListBuffer[NormalInverseWishart]],
          ): (Int,
    List[(Int,Int)],
    List[(DenseVector[Double], DenseMatrix[Double], Int)],
    List[List[(DenseVector[Double], DenseMatrix[Double], Int)]]) = {
    var NIWParamsByCol: ListBuffer[ListBuffer[NormalInverseWishart]] = global_NIWParamsByCol
    var it=1
    /*------------------------------------------------Col_partitioning----------------------------------------------*/
    /*-----------------------------------------------Variables------------------------------------------------------*/
    var local_col_partition: List[Int] = local_colPartition match {
      case Some(col) => col
      case None => List.fill(p)(0)
    }
    var countColCluster: ListBuffer[Int] = partitionToOrderedCount(local_col_partition).to[ListBuffer]
    /*----------------------------------------------Functions-------------------------------------------------------*/

    def removeElementFromColCluster(column: List[DenseVector[Double]], currentPartition: Int): Unit = {
      if (countColCluster(currentPartition) == 1) {
        countColCluster.remove(currentPartition)
        NIWParamsByCol.remove(currentPartition)
        local_col_partition = local_col_partition.map(c => {
          if (c > currentPartition) {
            c - 1
          } else c
        })
      } else {
        countColCluster.update(currentPartition, countColCluster.apply(currentPartition) - 1)
        (column zip rowPartition).groupBy(_._2).values.foreach(e => {
          val k = e.head._2
          val dataInCol = e.map(_._1)
          NIWParamsByCol(currentPartition).update(k,
            NIWParamsByCol(currentPartition)(k).removeObservations(dataInCol))
        })
      }
    }


    def addElementToColCluster(column: List[DenseVector[Double]],
                               newPartition: Int): Unit = {

      if (newPartition == countColCluster.length) {
        countColCluster = countColCluster ++ ListBuffer(1)
        val newCluster = (column zip rowPartition).groupBy(_._2).values.map(e => {
          val k = e.head._2
          val dataInRow = e.map(_._1)
          (k, this.prior.update(dataInRow))
        }).toList.sortBy(_._1).map(_._2).to[ListBuffer]
        NIWParamsByCol = NIWParamsByCol ++ ListBuffer(newCluster)
      } else {
        countColCluster.update(newPartition, countColCluster.apply(newPartition) + 1)
        (column zip rowPartition).groupBy(_._2).values.foreach(e => {
          val k = e.head._2
          val dataInCol = e.map(_._1)
          NIWParamsByCol(newPartition).update(k,
            NIWParamsByCol(newPartition)(k).update(dataInCol))
        })
      }
    }

    def updateColPartition(verbose: Boolean = false) = {
      for (i <- DataByCol.indices) {
        val currentData = DataByCol(i)
        val currentPartition = local_col_partition(i)
        removeElementFromColCluster(currentData, currentPartition)
        val newMembership = drawMembership(currentData, rowPartition, countColCluster, NIWParamsByCol, actualBeta)
        local_col_partition = local_col_partition.updated(i, newMembership)
        addElementToColCluster(currentData, newMembership)
      }
    }

    while (it <= maxIt) {
      updateColPartition()
      it=it+1
    }
    val col_sufficientStatistic = (DataByCol zip local_col_partition).groupBy(_._2).values.map(e => {
      val dataPeColCluster = e.map(_._1)
      dataPeColCluster.reduce(_ ++ _)
    }).toList.map(e => computeSufficientStatistics(e))
    (this.id,
      local_col_partition zip col_indices,
      col_sufficientStatistic,
      computeBlockSufficientStatistics(DataByCol, local_col_partition, rowPartition))
  }

  def runRow(maxIt:Int,
          local_rowPartition: Option[List[Int]]=None,
             colPartition:List[Int],
          global_NIWParamsByCol:ListBuffer[ListBuffer[NormalInverseWishart]]
          ): (Int,
    List[(Int,Int)],
    List[(DenseVector[Double], DenseMatrix[Double],Int)],
    List[List[(DenseVector[Double], DenseMatrix[Double],Int)]]) = {
    /*-----------------------------------------------Variables------------------------------------------------------*/
    var NIWParamsByCol: ListBuffer[ListBuffer[NormalInverseWishart]] = global_NIWParamsByCol
    var it=1
      /*-----------------------------------------------Row_partitioning----------------------------------------------*/
      /*-----------------------------------------------Variables-----------------------------------------------------*/
      var local_row_partition: List[Int] = local_rowPartition match {
        case Some(ro)=>ro
        case None=> List.fill(n)(0)
      }
      var countRowCluster: ListBuffer[Int] = partitionToOrderedCount(local_row_partition).to[ListBuffer]
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
            colPartition, countRowCluster, NIWParamsByCol.transpose, actualAlpha)
          local_row_partition = local_row_partition.updated(i, newPartition)
          addElementToRowCluster(currentData, newPartition)
        }
      }

      while(it<=maxIt)
        {
          updateRowPartition()
          it=it+1
        }
        val row_sufficientStatistic=(DataByRow zip local_row_partition).groupBy(_._2).values.map(e=>{
          val dataPerRowCluster = e.map(_._1)
          dataPerRowCluster.reduce(_ ++ _)
        }).toList.map(e=>computeSufficientStatistics(e))
      (this.id,
        local_row_partition zip row_indices,
        row_sufficientStatistic,
        computeBlockSufficientStatistics(DataByRowT,colPartition,local_row_partition))
    }

}
