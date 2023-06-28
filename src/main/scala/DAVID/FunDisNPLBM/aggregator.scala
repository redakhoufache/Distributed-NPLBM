package DAVID.FunDisNPLBM


import DAVID.Common.NormalInverseWishart
import DAVID.Common.ProbabilisticTools.{normalizeLogProbability, sample}
import breeze.linalg.{DenseMatrix, DenseVector, sum}
import breeze.numerics.{NaN, log}

import scala.collection.mutable.ListBuffer

class aggregator(actualAlpha: Double,
                 prior: NormalInverseWishart,
                 line_sufficientStatistic: List[(DenseVector[Double], DenseMatrix[Double], Int)],
                 map_paration:List[(Int,Int,Int)],
                 blockss:List[(Int,List[List[(DenseVector[Double], DenseMatrix[Double], Int)]])], N:Int,worker_id:Int
                 ) extends Serializable {
  val id=worker_id
  var local_map_paration=map_paration
  var local_blockss=blockss
  val weights: List[Int] = line_sufficientStatistic.map(e => e._3)
  var sum_weights:Int=weights.sum
  val means: List[DenseVector[Double]] = line_sufficientStatistic.map(e => e._1)
  val squaredSums: List[DenseMatrix[Double]] = line_sufficientStatistic.map(e => e._2)
  val Data: List[(DenseVector[Double], DenseMatrix[Double], Int)] = line_sufficientStatistic
  var cluster_partition_local: List[Int] = List.fill(means.size)(0)
  var cluster_partition: List[Int] = cluster_partition_local
  var NIWParams_local: ListBuffer[NormalInverseWishart] = (Data zip cluster_partition_local).groupBy(_._2).values.map(e => {
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
  /*============================================================================================*/
  def priorPredictive(idx: Int): Double = {
    prior.priorPredictiveFromSufficientStatistics(weights(idx), means(idx), squaredSums(idx))
  }

  def computeClusterMembershipProbabilities(idx: Int): List[Double] = {
    val d: Int = means.head.length
    NIWParams_local.indices.map(k => {
      (k,
        NIWParams_local(k).priorPredictiveFromSufficientStatistics(weights(idx), means(idx), squaredSums(idx))
          + log(NIWParams_local(k).nu - d)
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
    val formerMembership: Int = cluster_partition_local(idx)
    if (NIWParams_local(formerMembership).nu == weights(idx) + d + 1) {
      NIWParams_local.remove(formerMembership)
      cluster_partition_local = cluster_partition_local.map(c => {
        if (c > cluster_partition_local(idx)) {
          c - 1
        } else c
      })
    } else {
      val updatedNIWParams = NIWParams_local(formerMembership).removeFromSufficientStatistics(
        weights(idx),
        means(idx),
        squaredSums(idx)
      )
      NIWParams_local.update(formerMembership, updatedNIWParams)
    }
  }

  def addElementToCluster(idx: Int): Unit = {
    val newPartition = cluster_partition_local(idx)
    if (newPartition == NIWParams_local.length) {
      val newNIWparam = this.prior.updateFromSufficientStatistics(weights(idx), means(idx), squaredSums(idx))
      NIWParams_local = NIWParams_local ++ ListBuffer(newNIWparam)
    } else {
      val updatedNIWParams = NIWParams_local(newPartition).updateFromSufficientStatistics(
        weights(idx),
        means(idx),
        squaredSums(idx)
      )
      NIWParams_local.update(newPartition, updatedNIWParams)
    }
  }

  def updatePartition(): List[Int] = {
    for (idx <- means.indices) {
      removeElementFromCluster(idx)
      val newPartition = drawMembership(idx)
      cluster_partition_local = cluster_partition_local.updated(idx, newPartition)
      addElementToCluster(idx)
    }
    cluster_partition_local
  }
  /*============================================================================================*/
  var result:(List[Int], ListBuffer[ListBuffer[NormalInverseWishart]], List[List[Int]])=(List.fill(0)(0),
    new ListBuffer[ListBuffer[NormalInverseWishart]](),List.fill(1)(List.fill(0)(0)))
  private def aggregateMeans(ms: List[DenseVector[Double]], ws: List[Int]): DenseVector[Double] = {
    val sums = sum((ms zip ws).map(e => e._2.toDouble * e._1)) / sum(ws).toDouble
    sums
  }

  private def aggregateSquaredSums(sS: List[DenseMatrix[Double]],
                                   ms: List[DenseVector[Double]],
                                   ws: List[Int],
                                   aggMean: DenseVector[Double]): DenseMatrix[Double] = {
    val aggM: DenseMatrix[Double] =
      sum(sS) + sum((ms zip ws).map(e => (e._1 * e._1.t) * e._2.toDouble)) - sum(ws).toDouble * aggMean * aggMean.t
    aggM
  }


  private def map_local_global_partition(cluster_partition: List[Int],
                                         map_paration:List[(Int, Int)]):
  List[(Int, Int, Int)] = {
    (map_paration.sortBy(_._1) zip cluster_partition).map(e=>(e._1._1,e._1._2,e._2))
  }

  def global_line_partition(
                             workerResultsCompact: List[(Int, Int,Int)],
                             map_localPart_globalPart: List[(Int, Int, Int)]): List[List[(Int, Int, Int)]] = {
    workerResultsCompact.groupBy(_._1).map(_._2).map(_.map(e=>{
      (e._1,map_localPart_globalPart.filter(x=>{(x._1==e._1 && x._2==e._2)}).head._3,e._2)
    })).toList
  }

  private def global_NIW_row(
                              map_localPart_globalPart: List[(Int, Int, Int)],
                              countRow: Int,
                              countCol: Int,
                              BlockSufficientStatistics: List[List[List[(DenseVector[Double], DenseMatrix[Double], Int)]]]):
  ListBuffer[ListBuffer[NormalInverseWishart]] = {
    var result: ListBuffer[ListBuffer[NormalInverseWishart]] = ListBuffer()
    for (i <- 0 to countCol) {
      val row_NIWs: ListBuffer[NormalInverseWishart] = ListBuffer()
      for (j <- 0 to countRow) {
        val map_SufficientStatistics_j = map_localPart_globalPart.filter(_._3 == j).map(e => {
          (e._1, e._2)
        })
        val SufficientStatistics_j = map_SufficientStatistics_j.indices.map(index_row => {
          val tmp = map_SufficientStatistics_j(index_row)
          val t1=BlockSufficientStatistics(tmp._1)(i)
          val t3=t1(tmp._2)
          t3
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
          val tmp=map_SufficientStatistics_j(index_row)
          BlockSufficientStatistics(tmp._1)(tmp._2)(j)
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

  def run():aggregator={
    cluster_partition_local=updatePartition()
    cluster_partition= cluster_partition_local
    this
  }
  def runCol(
              nIter: Int = 1,
              partitionOtherDimension: List[Int],
              worker:aggregator):  aggregator= {
    sum_weights+=worker.sum_weights
    local_map_paration=local_map_paration++worker.local_map_paration
    local_blockss=local_blockss ++ worker.local_blockss
    cluster_partition= cluster_partition ++ List.fill(worker.cluster_partition.size)(0)


    val d: Int = worker.NIWParams_local.head.mu.length
    var it = 1

    /*--------------------------------------------Functions-----------------------------------------------------*/
    def priorPredictive(weight:Int,mean:DenseVector[Double],squaredSum:DenseMatrix[Double]): Double = {
      prior.priorPredictiveFromSufficientStatistics(weight, mean, squaredSum)
    }

    def computeClusterMembershipProbabilities(weight:Int,mean:DenseVector[Double],squaredSum:DenseMatrix[Double]): List[Double] = {
      val d: Int = mean.length
      NIWParams_local.indices.map(k => {
        (k,
          NIWParams_local(k).priorPredictiveFromSufficientStatistics(weight, mean, squaredSum)
            + log(NIWParams_local(k).nu - d)
        )
      }).toList.sortBy(_._1).map(_._2)
    }

    def drawMembership(weight:Int,mean:DenseVector[Double],squaredSum:DenseMatrix[Double]): Int = {

      val probPartition = computeClusterMembershipProbabilities(weight, mean, squaredSum)
      val posteriorPredictiveXi = priorPredictive(weight, mean, squaredSum)
      val probs = probPartition :+ (posteriorPredictiveXi + log(actualAlpha))
      val normalizedProbs = normalizeLogProbability(probs)
      sample(normalizedProbs)
    }

    def addElementToCluster(weight:Int,mean:DenseVector[Double],squaredSum:DenseMatrix[Double],newPartition:Int,index:Int): Unit = {
      if (newPartition == NIWParams_local.length) {
        val newNIWparam = this.prior.updateFromSufficientStatistics(weight, mean, squaredSum)
        NIWParams_local = NIWParams_local ++ ListBuffer(newNIWparam)
      } else {
        val updatedNIWParams = NIWParams_local(newPartition).updateFromSufficientStatistics(
          weight, mean, squaredSum
        )
        NIWParams_local.update(newPartition, updatedNIWParams)
      }
      cluster_partition=cluster_partition.updated(index,newPartition)
    }

    def JointWorkers(worker:aggregator): List[Int] = {
      worker.NIWParams_local.indices.map(i=>{
        val NIW=worker.NIWParams_local(i)
        val newPartition = drawMembership(weight=NIW.nu,mean = NIW.mu,squaredSum = NIW.psi)
        addElementToCluster(weight=NIW.nu,mean = NIW.mu,squaredSum = NIW.psi,newPartition = newPartition,index=cluster_partition.size-worker.cluster_partition.size+i)
      })
      cluster_partition
    }
    JointWorkers(worker)
    if(sum_weights==N)
    {
      val local_line_partition=local_map_paration.map(e => {
        (e._1, e._2)
      }).distinct
      val map_cluster_Partition = map_local_global_partition(cluster_partition,
        map_paration = local_line_partition)

      val globalLinePartition = global_line_partition(workerResultsCompact = local_map_paration, map_cluster_Partition)
      val col_partition = globalLinePartition.reduce(_ ++ _).sortBy(_._1).map(_._2)
      val global_NIW_s = global_NIW_col(
        map_localPart_globalPart = map_cluster_Partition,
        countRow = partitionOtherDimension.max,
        countCol = cluster_partition.max,
        BlockSufficientStatistics =local_blockss.sortBy(_._1).map(_._2))
      val local_Col_paratitions = globalLinePartition.map(e => {
        e.sortBy(_._1).map(_._2)
      })
      require(col_partition.size==N,s"error ${col_partition.size}")
      result=(col_partition, global_NIW_s,local_Col_paratitions)
    }

    this
  }

  def runRow(
              nIter: Int = 1,
              partitionOtherDimension: List[Int],
              worker: aggregator): aggregator = {
    sum_weights += worker.sum_weights
    local_map_paration = local_map_paration ++ worker.local_map_paration
    local_blockss = local_blockss ++ worker.local_blockss
    cluster_partition = cluster_partition ++ List.fill(worker.cluster_partition.size)(0)


    val d: Int = worker.NIWParams_local.head.mu.length
    var it = 1

    /*--------------------------------------------Functions-----------------------------------------------------*/
    def priorPredictive(weight: Int, mean: DenseVector[Double], squaredSum: DenseMatrix[Double]): Double = {
      prior.priorPredictiveFromSufficientStatistics(weight, mean, squaredSum)
    }

    def computeClusterMembershipProbabilities(weight: Int, mean: DenseVector[Double], squaredSum: DenseMatrix[Double]): List[Double] = {
      val d: Int = mean.length
      NIWParams_local.indices.map(k => {
        (k,
          NIWParams_local(k).priorPredictiveFromSufficientStatistics(weight, mean, squaredSum)
            + log(NIWParams_local(k).nu - d)
        )
      }).toList.sortBy(_._1).map(_._2)
    }

    def drawMembership(weight: Int, mean: DenseVector[Double], squaredSum: DenseMatrix[Double]): Int = {

      val probPartition = computeClusterMembershipProbabilities(weight, mean, squaredSum)
      val posteriorPredictiveXi = priorPredictive(weight, mean, squaredSum)
      val probs = probPartition :+ (posteriorPredictiveXi + log(actualAlpha))
      val normalizedProbs = normalizeLogProbability(probs)
      sample(normalizedProbs)
    }

    def addElementToCluster(weight: Int, mean: DenseVector[Double], squaredSum: DenseMatrix[Double], newPartition: Int, index: Int): Unit = {
      if (newPartition == NIWParams_local.length) {
        val newNIWparam = this.prior.updateFromSufficientStatistics(weight, mean, squaredSum)
        NIWParams_local = NIWParams_local ++ ListBuffer(newNIWparam)
      } else {
        val updatedNIWParams = NIWParams_local(newPartition).updateFromSufficientStatistics(
          weight, mean, squaredSum
        )
        NIWParams_local.update(newPartition, updatedNIWParams)
      }
      cluster_partition = cluster_partition.updated(index, newPartition)
    }

    def JointWorkers(worker: aggregator): List[Int] = {
      worker.NIWParams_local.indices.map(i => {
        val NIW = worker.NIWParams_local(i)
        val newPartition = drawMembership(weight = NIW.nu, mean = NIW.mu, squaredSum = NIW.psi)
        addElementToCluster(weight = NIW.nu, mean = NIW.mu, squaredSum = NIW.psi, newPartition = newPartition, index = cluster_partition.size - worker.cluster_partition.size + i)
      })
      cluster_partition
    }

    JointWorkers(worker)
    if (sum_weights == N) {
      val local_line_partition = local_map_paration.map(e => {
        (e._1, e._2)
      }).distinct
      val map_cluster_Partition = map_local_global_partition(cluster_partition,
        map_paration = local_line_partition)

      val globalLinePartition = global_line_partition(workerResultsCompact = local_map_paration, map_cluster_Partition)
      val row_partition = globalLinePartition.reduce(_ ++ _).sortBy(_._1).map(_._2)
      val global_NIW_s = global_NIW_row(
        map_localPart_globalPart = map_cluster_Partition,
        countRow = cluster_partition.max,
        countCol = partitionOtherDimension.max,
        BlockSufficientStatistics = local_blockss.sortBy(_._1).map(_._2))
      val local_Col_paratitions = globalLinePartition.map(e => {
        e.sortBy(_._1).map(_._2)
      })
      require(row_partition.size == N, s"error ${row_partition.size}")
      result = (row_partition, global_NIW_s, local_Col_paratitions)
    }

    this
  }
}
