package DAVID.FunDisNPLBM

import DAVID.Common.NormalInverseWishart
import DAVID.Common.ProbabilisticTools.{normalizeLogProbability, sample}
import breeze.linalg.{DenseMatrix, DenseVector, sum}
import breeze.numerics.log

import scala.collection.mutable.ListBuffer
/*workerResultsCompact should be ordered by workerId _._1 */

class MasterNPLBM (actualAlpha: Double, prior: NormalInverseWishart,N:Int,P:Int) extends Serializable {

  private def aggregateMeans(ms: List[DenseVector[Double]], ws: List[Int]): DenseVector[Double] = {
    val sums = sum((ms zip ws).map(e=>e._2.toDouble * e._1))/sum(ws).toDouble
    sums
  }

  private def aggregateSquaredSums(sS: List[DenseMatrix[Double]],
                           ms: List[DenseVector[Double]],
                           ws: List[Int],
                           aggMean: DenseVector[Double]): DenseMatrix[Double] = {
    val aggM: DenseMatrix[Double] =
      sum(sS) + sum((ms zip ws).map(e=>(e._1 * e._1.t) * e._2.toDouble)) - sum(ws).toDouble * aggMean * aggMean.t
    aggM
  }




  private def map_local_global_partition(cluster_partition: List[Int],
                                         workerResultsCompact: List[(Int,
                                           List[(Int, Int)],
                                           List[(DenseVector[Double], DenseMatrix[Double], Int)],
                                           List[List[(DenseVector[Double], DenseMatrix[Double], Int)]])]):
  List[(Int,Int,Int)]={

    val tmp=workerResultsCompact.par.flatten {
      case (workerId: Int,
      _: List[(Int, Int)],
      line_ss: List[(DenseVector[Double], DenseMatrix[Double], Int)],
      _: List[List[(DenseVector[Double], DenseMatrix[Double], Int)]]) =>
        line_ss.indices.map(i => {
          (workerId, i)
        })
    }.toList.sortBy(_._1)
      tmp.indices.map(i=>{
      (tmp(i)._1,tmp(i)._2,cluster_partition(i))
    }).toList
  }
   def global_line_partition(
                             workerResultsCompact: List[(Int,
                               List[(Int, Int)],
                               List[(DenseVector[Double], DenseMatrix[Double], Int)],
                               List[List[(DenseVector[Double], DenseMatrix[Double], Int)]])],
                             map_localPart_globalPart:List[(Int,Int,Int)]): List[List[(Int,Int,Int)]]={
      /*map_localPart_globalPart (workerId,local_clusterId,global_clusterId)*/
    val test=workerResultsCompact.sortBy(_._1).map(e => {
      val tmp_2 = map_localPart_globalPart.filter(_._1==e._1)
      e._2.indices.map(index => {
        val old_element = e._2(index)
        val tmpList=tmp_2.filter(_._2 == old_element._1)
        val element = (tmpList.head._3, old_element._2)
        (element._2,element._1,old_element._1)
      }).toList
    })
     test
  }
  private def global_NIW_row(
                  map_localPart_globalPart:List[(Int,Int,Int)],
                  countRow:Int,
                  countCol:Int,
                  BlockSufficientStatistics:List[List[List[(DenseVector[Double], DenseMatrix[Double], Int)]]]):
  ListBuffer[ListBuffer[NormalInverseWishart]]={
    var result:ListBuffer[ListBuffer[NormalInverseWishart]]=ListBuffer()
    for(i<- 0 to countCol){
      val row_NIWs: ListBuffer[NormalInverseWishart] = ListBuffer()
      for (j <- 0 to countRow) {
        val map_SufficientStatistics_j = map_localPart_globalPart.filter(_._3 == j).map(e => {
          (e._1, e._2)
        })
        val SufficientStatistics_j = map_SufficientStatistics_j.indices.map(index_row => {
          val tmp=map_SufficientStatistics_j(index_row)
          BlockSufficientStatistics(tmp._1)(i)(tmp._2)
        }).toList
        val meansPerCluster = SufficientStatistics_j.map(_._1)
        val weightsPerCluster = SufficientStatistics_j.map(_._3)
        val aggregatedMeans: DenseVector[Double] = aggregateMeans(meansPerCluster, weightsPerCluster)
        val squaredSumsPerCluster: List[DenseMatrix[Double]] = SufficientStatistics_j.map(_._2)
        val aggregatedsS: DenseMatrix[Double] = aggregateSquaredSums(
          sS = squaredSumsPerCluster,
          ms = meansPerCluster,
          ws = weightsPerCluster,
          aggMean = aggregatedMeans
        )

        row_NIWs.append(prior.updateFromSufficientStatistics(
          weight = sum(weightsPerCluster),
          mean = aggregatedMeans,
          SquaredSum = aggregatedsS
        ))
      }
      result.append(row_NIWs)
    }
    result
  }

  private def global_NIW_col(
                  map_localPart_globalPart: List[(Int, Int, Int)],
                  countRow: Int,
                  countCol: Int,
                  BlockSufficientStatistics: List[List[List[(DenseVector[Double], DenseMatrix[Double], Int)]]]):
  ListBuffer[ListBuffer[NormalInverseWishart]] = {
    val result: ListBuffer[ListBuffer[NormalInverseWishart]] = ListBuffer()
    for (i <- 0 to countCol) {
      var row_NIWs: ListBuffer[NormalInverseWishart] = ListBuffer()
      val map_SufficientStatistics_j = map_localPart_globalPart.filter(_._3 == i).map(e => {
        (e._1, e._2)
      })
      for (j <- 0 to countRow) {
        val SufficientStatistics_j = map_SufficientStatistics_j.indices.map(index_row => {
          BlockSufficientStatistics(map_SufficientStatistics_j(index_row)._1)(map_SufficientStatistics_j(index_row)._2)(j)
        }).toList
        val meansPerCluster = SufficientStatistics_j.map(_._1)
        val weightsPerCluster = SufficientStatistics_j.map(_._3)
        val aggregatedMeans: DenseVector[Double] = aggregateMeans(meansPerCluster, weightsPerCluster)
        val squaredSumsPerCluster: List[DenseMatrix[Double]] = SufficientStatistics_j.map(_._2)
        val aggregatedsS: DenseMatrix[Double] = aggregateSquaredSums(
          sS = squaredSumsPerCluster,
          ms = meansPerCluster,
          ws = weightsPerCluster,
          aggMean = aggregatedMeans
        )

        row_NIWs.append(prior.updateFromSufficientStatistics(
          weight = sum(weightsPerCluster),
          mean = aggregatedMeans,
          SquaredSum = aggregatedsS
        ))
      }
      result.append(row_NIWs)
    }
    result
  }
  def runRow(
              nIter: Int = 1,
              partitionOtherDimension: List[Int],
              workerResultsCompact: List[(Int, List[(Int, Int)],
                List[(DenseVector[Double], DenseMatrix[Double], Int)],
                List[List[(DenseVector[Double], DenseMatrix[Double], Int)]])],
              verbose: Boolean = false): (List[Int], ListBuffer[ListBuffer[NormalInverseWishart]],List[List[Int]]) = {
    val workerResults: List[(Int, Int, (DenseVector[Double], DenseMatrix[Double]), Int)] =
      workerResultsCompact.par.flatten {
      case(workerId:Int,
      _:List[(Int,Int)],
      line_ss:List[(DenseVector[Double], DenseMatrix[Double], Int)],
      _:List[List[(DenseVector[Double], DenseMatrix[Double], Int)]]) =>
        line_ss.indices.map(i => {
          (workerId,
            i,
            (line_ss(i)._1, line_ss(i)._2), line_ss(i)._3)
        })
    }.toList.sortBy(_._1)
    val n: Int = workerResults.length

    val weights: List[Int] = workerResults.map(e => e._4)
    val means: List[DenseVector[Double]] = workerResults.map(e => e._3._1)
    val squaredSums: List[DenseMatrix[Double]] = workerResults.map(e => e._3._2)
    val Data: List[(DenseVector[Double], DenseMatrix[Double], Int)] = (0 until n).toList.map(i => (
      means(i),
      squaredSums(i),
      weights(i))
    )
     var cluster_partition: List[Int] = List.fill(n)(0)

    var NIWParams: ListBuffer[NormalInverseWishart] = (Data zip cluster_partition).groupBy(_._2).values.map(e => {
      val DataPerCluster: List[(DenseVector[Double], DenseMatrix[Double], Int)] = e.map(_._1)
      val clusterIdx = e.head._2
      val meansPerCluster = DataPerCluster.map(_._1)
      val weightsPerCluster = DataPerCluster.map(_._3)
      val aggregatedMeans: DenseVector[Double] = aggregateMeans(meansPerCluster, weightsPerCluster)
      val squaredSumsPerCluster: List[DenseMatrix[Double]] = DataPerCluster.map(_._2)
      val aggregatedsS: DenseMatrix[Double] = aggregateSquaredSums(
        sS = squaredSumsPerCluster,
        ms = meansPerCluster,
        ws = weightsPerCluster,
        aggMean = aggregatedMeans
      )
      (clusterIdx, prior.updateFromSufficientStatistics(
        weight = sum(weightsPerCluster),
        mean = aggregatedMeans,
        SquaredSum = aggregatedsS
      )
      )
    }).toList.sortBy(_._1).map(_._2).to[ListBuffer]
    val d: Int = means.head.length
    var it = 1

    /*----------------------------------------------Functions-----------------------------------------------------*/
    def priorPredictive(idx: Int): Double = {
      prior.priorPredictiveFromSufficientStatistics(weights(idx), means(idx), squaredSums(idx))
    }

    def computeClusterMembershipProbabilities(idx: Int): List[Double] = {
      val d: Int = means.head.length
      NIWParams.indices.par.map(k => {
        (k,
          NIWParams(k).priorPredictiveFromSufficientStatistics(weights(idx), means(idx), squaredSums(idx))
            + log(NIWParams(k).nu - d)
        )
      }).toList.sortBy(_._1).map(_._2)
    }

    def drawMembership(idx: Int): Int = {

      val probPartition = computeClusterMembershipProbabilities(idx)
      val posteriorPredictiveXi = priorPredictive(idx)
      val probs = probPartition :+ (posteriorPredictiveXi + log(actualAlpha))
      val normalizedProbs = normalizeLogProbability(probs)
      sample(normalizedProbs)
    }
     def removeElementFromCluster(idx: Int): Unit = {
       val d: Int = means.head.length
      val formerMembership: Int = cluster_partition(idx)
      if (NIWParams(formerMembership).nu == weights(idx) + d + 1) {
        NIWParams.remove(formerMembership)
        cluster_partition = cluster_partition.map(c => {
          if (c > cluster_partition(idx)) {
            c - 1
          } else c
        })
      } else {
        val updatedNIWParams = NIWParams(formerMembership).removeFromSufficientStatistics(
          weights(idx),
          means(idx),
          squaredSums(idx)
        )
        NIWParams.update(formerMembership, updatedNIWParams)
      }
    }

    def addElementToCluster(idx: Int): Unit = {
      val newPartition = cluster_partition(idx)
      if (newPartition == NIWParams.length) {
        val newNIWparam = this.prior.updateFromSufficientStatistics(weights(idx), means(idx), squaredSums(idx))
        NIWParams = NIWParams ++ ListBuffer(newNIWparam)
      } else {
        val updatedNIWParams = NIWParams(newPartition).updateFromSufficientStatistics(
          weights(idx),
          means(idx),
          squaredSums(idx)
        )
        NIWParams.update(newPartition, updatedNIWParams)
      }
    }

    def updatePartition(): List[Int] = {
      for (idx <- means.indices) {
        removeElementFromCluster(idx)
        val newPartition = drawMembership(idx)
        cluster_partition = cluster_partition.updated(idx, newPartition)
        addElementToCluster(idx)
      }
      cluster_partition
    }
    while (it<=nIter)
      {
        if (verbose) {
          println("\n Clustering >>>>>> Iteration: " + it.toString)

/*
          println(NIWParams.map(_.nu - d).toList)
*/
        }
        updatePartition()
        it=it+1
      }
    val map_cluster_Partition=map_local_global_partition(cluster_partition,workerResultsCompact)
    val globalLinePartition=global_line_partition(workerResultsCompact,map_cluster_Partition)
    val row_partition=globalLinePartition.reduce(_ ++ _).sortBy(_._1).map(_._2)

    val global_NIW_s=global_NIW_row(
      map_localPart_globalPart = map_cluster_Partition,
      countRow = cluster_partition.max,
      countCol = partitionOtherDimension.max,
      BlockSufficientStatistics = workerResultsCompact.map(_._4))
    val local_row_paratitions=globalLinePartition.map(e=>{
      e.sortBy(_._1).map(_._2)
    })
    (row_partition, global_NIW_s,local_row_paratitions)

  }

  def runCol(
              nIter: Int = 1,
              partitionOtherDimension: List[Int],
              workerResultsCompact: List[(Int,
                List[(Int, Int)],
                List[(DenseVector[Double], DenseMatrix[Double], Int)],
                List[List[(DenseVector[Double], DenseMatrix[Double], Int)]])],
              verbose: Boolean = false): (List[Int], ListBuffer[ListBuffer[NormalInverseWishart]],List[List[Int]]) = {
    val workerResults: List[(Int, Int, (DenseVector[Double], DenseMatrix[Double]), Int)] =
      workerResultsCompact.par.flatten{
        case (workerId: Int,
      _: List[(Int, Int)],
      line_ss: List[(DenseVector[Double], DenseMatrix[Double], Int)],
      _: List[List[(DenseVector[Double], DenseMatrix[Double], Int)]]) =>
        line_ss.indices.map(i => {
          (workerId,
            i,
            (line_ss(i)._1, line_ss(i)._2), line_ss(i)._3)
        })
      }.toList.sortBy(_._1)
    val n: Int = workerResults.length

    val weights: List[Int] = workerResults.map(e => e._4)
    val means: List[DenseVector[Double]] = workerResults.map(e => e._3._1)
    val squaredSums: List[DenseMatrix[Double]] = workerResults.map(e => e._3._2)
    val Data: List[(DenseVector[Double], DenseMatrix[Double], Int)] = (0 until n).toList.map(i => (
      means(i),
      squaredSums(i),
      weights(i))
    )
    var cluster_partition: List[Int] = List.fill(n)(0)

    var NIWParams: ListBuffer[NormalInverseWishart] = (Data zip cluster_partition).groupBy(_._2).values.map(e => {
      val DataPerCluster: List[(DenseVector[Double], DenseMatrix[Double], Int)] = e.map(_._1)
      val clusterIdx = e.head._2
      val meansPerCluster = DataPerCluster.map(_._1)
      val weightsPerCluster = DataPerCluster.map(_._3)
      val aggregatedMeans: DenseVector[Double] = aggregateMeans(meansPerCluster, weightsPerCluster)
      val squaredSumsPerCluster: List[DenseMatrix[Double]] = DataPerCluster.map(_._2)
      val aggregatedsS: DenseMatrix[Double] = aggregateSquaredSums(
        sS = squaredSumsPerCluster,
        ms = meansPerCluster,
        ws = weightsPerCluster,
        aggMean = aggregatedMeans
      )
      (clusterIdx, prior.updateFromSufficientStatistics(
        weight = sum(weightsPerCluster),
        mean = aggregatedMeans,
        SquaredSum = aggregatedsS
      )
      )
    }).toList.sortBy(_._1).map(_._2).to[ListBuffer]
    val d: Int = means.head.length
    var it = 1

    /*--------------------------------------------Functions-----------------------------------------------------*/
    def priorPredictive(idx: Int): Double = {
      prior.priorPredictiveFromSufficientStatistics(weights(idx), means(idx), squaredSums(idx))
    }

    def computeClusterMembershipProbabilities(idx: Int): List[Double] = {
      val d: Int = means.head.length
      NIWParams.indices.par.map(k => {
        (k,
          NIWParams(k).priorPredictiveFromSufficientStatistics(weights(idx), means(idx), squaredSums(idx))
            + log(NIWParams(k).nu - d)
        )
      }).toList.sortBy(_._1).map(_._2)
    }

    def drawMembership(idx: Int): Int = {

      val probPartition = computeClusterMembershipProbabilities(idx)
      val posteriorPredictiveXi = priorPredictive(idx)
      val probs = probPartition :+ (posteriorPredictiveXi + log(actualAlpha))
      val normalizedProbs = normalizeLogProbability(probs)
      sample(normalizedProbs)
    }
     def removeElementFromCluster(idx: Int): Unit = {
       val d: Int = means.head.length
      val formerMembership: Int = cluster_partition(idx)
      if (NIWParams(formerMembership).nu == weights(idx) + d + 1) {
        NIWParams.remove(formerMembership)
        cluster_partition = cluster_partition.map(c => {
          if (c > cluster_partition(idx)) {
            c - 1
          } else c
        })
      } else {
        val updatedNIWParams = NIWParams(formerMembership).removeFromSufficientStatistics(
          weights(idx),
          means(idx),
          squaredSums(idx)
        )
        NIWParams.update(formerMembership, updatedNIWParams)
      }
    }

    def addElementToCluster(idx: Int): Unit = {
      val newPartition = cluster_partition(idx)
      if (newPartition == NIWParams.length) {
        val newNIWparam = this.prior.updateFromSufficientStatistics(weights(idx), means(idx), squaredSums(idx))
        NIWParams = NIWParams ++ ListBuffer(newNIWparam)
      } else {
        val updatedNIWParams = NIWParams(newPartition).updateFromSufficientStatistics(
          weights(idx),
          means(idx),
          squaredSums(idx)
        )
        NIWParams.update(newPartition, updatedNIWParams)
      }
    }

    def updatePartition(): List[Int] = {
      for (idx <- means.indices) {
        removeElementFromCluster(idx)
        val newPartition = drawMembership(idx)
        cluster_partition = cluster_partition.updated(idx, newPartition)
        addElementToCluster(idx)
      }
      cluster_partition
    }
    while (it <= nIter) {
      if (verbose) {
        println("\n Clustering >>>>>> Iteration: " + it.toString)
        println(NIWParams.map(_.nu - d).toList)
      }
      updatePartition()
      it=it+1
    }

    val map_cluster_Partition = map_local_global_partition(cluster_partition, workerResultsCompact)
    val globalLinePartition = global_line_partition(workerResultsCompact, map_cluster_Partition)
    val col_partition = globalLinePartition.reduce(_ ++ _).sortBy(_._1).map(_._2)
    val global_NIW_s = global_NIW_col(
      map_localPart_globalPart = map_cluster_Partition,
      countRow = partitionOtherDimension.max ,
      countCol = cluster_partition.max ,
      BlockSufficientStatistics = workerResultsCompact.map(_._4))
    val local_Col_paratitions = globalLinePartition.map(e => {
      e.sortBy(_._1).map(_._2)
    })
      (col_partition, global_NIW_s,local_Col_paratitions)
  }
}
