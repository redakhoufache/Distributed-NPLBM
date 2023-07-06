package DAVID.FunDisNPLBM


import DAVID.Common.NormalInverseWishart
import DAVID.Common.ProbabilisticTools.{normalizeLogProbability, sample}
import breeze.linalg.{DenseMatrix, DenseVector, sum}
import breeze.numerics.log
import scala.collection.immutable.SortedMap
import scala.collection.mutable.ListBuffer

class aggregator(actualAlpha: Double,
                 prior: NormalInverseWishart,
                 line_sufficientStatistic: List[(DenseVector[Double], DenseMatrix[Double], Int)],
                 map_partition:ListBuffer[(Int,Int,Int)],
                 blockss:ListBuffer[(Int,List[List[(DenseVector[Double], DenseMatrix[Double], Int)]])], N:Int,worker_id:Int
                 ) extends Serializable {
  val id: Int =worker_id
  private var local_map_partition=map_partition
  private var local_blockss=blockss
  val weights: List[Int] = line_sufficientStatistic.map(e => e._3)
  private var sum_weights:Int=weights.sum
  val means: List[DenseVector[Double]] = line_sufficientStatistic.map(e => e._1)
  val squaredSums: List[DenseMatrix[Double]] = line_sufficientStatistic.map(e => e._2)
  val Data: List[(DenseVector[Double], DenseMatrix[Double], Int)] = line_sufficientStatistic
  private var cluster_partition_local: ListBuffer[(Int,Int)] = List.fill(means.size)((id,0)).to[ListBuffer]
  var cluster_partition: ListBuffer[(Int,Int)] = cluster_partition_local
  var NIWParams_local: ListBuffer[NormalInverseWishart] = (Data zip cluster_partition_local.map(_._2)).groupBy(_._2)
    .values.map(e => {
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
  private def priorPredictive(idx: Int): Double = {
    prior.priorPredictiveFromSufficientStatistics(weights(idx), means(idx), squaredSums(idx))
  }

  private def computeClusterMembershipProbabilities(idx: Int): List[Double] = {
    val d: Int = means.head.length
    NIWParams_local.indices.map(k => {
      (k,
        NIWParams_local(k).priorPredictiveFromSufficientStatistics(weights(idx), means(idx), squaredSums(idx))
          + log(NIWParams_local(k).nu - d)
      )
    }).toList.sortBy(_._1).map(_._2)
  }

  private def drawMembership(idx: Int): Int = {

    val probPartition = computeClusterMembershipProbabilities(idx)
    val posteriorPredictiveXi = priorPredictive(idx)
    val probs = probPartition :+ (posteriorPredictiveXi + log(actualAlpha))
    val normalizedProbs = normalizeLogProbability(probs)
    sample(normalizedProbs)
  }

  private def removeElementFromCluster(idx: Int): Unit = {
    val d: Int = means.head.length
    val formerMembership: Int = cluster_partition_local(idx)._2
    if (NIWParams_local(formerMembership).nu == weights(idx) + d + 1) {
      NIWParams_local.remove(formerMembership)
      cluster_partition_local = cluster_partition_local.map(c => {
        if (c._2 > cluster_partition_local(idx)._2) {
          (c._1,c._2 - 1)
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

  private def addElementToCluster(idx: Int): Unit = {
    val newPartition = cluster_partition_local(idx)._2
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

  private def updatePartition(): ListBuffer[(Int,Int)] = {
    for (idx <- means.indices) {
      removeElementFromCluster(idx)
      val newPartition = drawMembership(idx)
      cluster_partition_local = cluster_partition_local.updated(idx, (id,newPartition))
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
  /**
   *
   * */

  private def map_local_global_partition(cluster_partition: List[Int],
                                         map_partition:List[(Int, Int)]):
  List[(Int, Int, Int)] = {
    (map_partition zip cluster_partition).map(e=>(e._1._1,e._1._2,e._2))
  }
  /**
   * Ds: global_line_partition computes the global line (row or column) membership using worker membership
   * Input:workerResultsCompact List(worker_id,local_membership,line_index) of size number_of_lines
   *       map_localPart_globalPart List(worker_id,local_membership,global_membership) of size number_of_clusters
   * Output: List(List(worker_id,local_membership,global_membership)) for size (number_of_clusters * number_of_lines)
   * */
  private def global_line_partition(
                             workerResultsCompact: List[(Int, Int,Int)],
                             map_localPart_globalPart: List[(Int, Int, Int)]): List[List[(Int, Int)]] = {
    val result=workerResultsCompact.par.map(e=>{
      (e._1,e._3,map_localPart_globalPart.filter(x=>{x._1==e._1 && x._2==e._2}).head._3)
    }).toList
    val result1=SortedMap(result.groupBy(_._1).toSeq: _*).values.toList
    result1.par.map(_.map(e=>{(e._2,e._3)}).toList).toList
  }
  /**
   * Ds: global_NIW_row computes global blocks' NIWs after row clustering using workers blocks' sufficient statistics
   * Input: map_localPart_globalPart List[(worker_id,local_cluster_id,global_cluster_id)]
   *        countRow : Int = number of row clusters,
   *        countCol :Int = number of columns clusters
   *        BlockSufficientStatistics : List(List(List((mean,covariance,weight)))) the first list represent the workers,
   *        the second one represent column wise matrix sufficient statistics
   * Output : Global column wise  NIWs matrix
   * */
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

  /**
   * Ds: global_NIW_col computes global blocks' NIWs after columns clustering using workers blocks' sufficient statistics
   * Input: map_localPart_globalPart List[(worker_id,local_cluster_id,global_cluster_id)]
   * countRow : Int = number of row clusters,
   * countCol :Int = number of columns clusters
   * BlockSufficientStatistics : List(List(List((mean,covariance,weight)))) the first list represent the workers,
   * the second one represent column wise matrix sufficient statistics
   * Output : Global column wise  NIWs matrix
   * */
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

  /**
   * Ds: run computes line clusters in the workers using lines' sufficient statistics
   * Output : aggregator
   * */
  def run():aggregator={
    cluster_partition_local=updatePartition()
    cluster_partition= cluster_partition_local
    this
  }


  private def priorPredictive(weight: Int, mean: DenseVector[Double], squaredSum: DenseMatrix[Double]): Double = {
    prior.priorPredictiveFromSufficientStatistics(weight, mean, squaredSum)
  }

  private def computeClusterMembershipProbabilities(weight: Int, mean: DenseVector[Double], squaredSum: DenseMatrix[Double]): List[Double] = {
    val d: Int = mean.length
    NIWParams_local.indices.map(k => {
      (k,
        NIWParams_local(k).priorPredictiveFromSufficientStatistics(weight, mean, squaredSum)
          + log(NIWParams_local(k).nu - d)
      )
    }).toList.sortBy(_._1).map(_._2)
  }

  /**
   * Ds: drawMembership computes the probabilities of choosing existing clusters or a new cluster for a line cluster
   * Input : mean, weight and covariance of line cluster
   * Output : sample : Int
   * */
  private def drawMembership(weight: Int, mean: DenseVector[Double], squaredSum: DenseMatrix[Double]): Int = {

    val probPartition = computeClusterMembershipProbabilities(weight, mean, squaredSum)
    val posteriorPredictiveXi = priorPredictive(weight, mean, squaredSum)
    val probs = probPartition :+ (posteriorPredictiveXi + log(actualAlpha))
    val normalizedProbs = normalizeLogProbability(probs)
    sample(normalizedProbs)
  }

  private def addElementToCluster(weight: Int, mean: DenseVector[Double], squaredSum: DenseMatrix[Double], newPartition: Int, index: Int): Unit = {
    if (newPartition == NIWParams_local.length) {
      val newNIWparam = this.prior.updateFromSufficientStatistics(weight, mean, squaredSum)
      NIWParams_local = NIWParams_local ++ ListBuffer(newNIWparam)
    } else {
      val updatedNIWParams = NIWParams_local(newPartition).updateFromSufficientStatistics(
        weight, mean, squaredSum
      )
      NIWParams_local.update(newPartition, updatedNIWParams)
    }
    val local_id = cluster_partition(index)._1
    cluster_partition = cluster_partition.updated(index, (local_id, newPartition))
  }

  /**
   * Ds: JointWorkers clusters the DPMM's result of an a aggregator into an other one
   * Input : aggregator
   * Output : cluster line membership (DPMM result)
   * */
  private def JointWorkers(worker: aggregator): ListBuffer[(Int, Int)] = {
    worker.NIWParams_local.indices.foreach(i => {
      val NIW = worker.NIWParams_local(i)
      val newPartition = drawMembership(weight = NIW.nu, mean = NIW.mu, squaredSum = NIW.psi)
      addElementToCluster(weight = NIW.nu, mean = NIW.mu, squaredSum = NIW.psi, newPartition = newPartition, index = cluster_partition.size - worker.cluster_partition.size + i)
    })
    cluster_partition
  }

  /**
   * Ds: runCol execute streaming DPMM on the columns result of workers  (clustering of local columns clusters)
   * Input : partitionOtherDimension and worker
   * Output : aggregator
   * */
  def runCol(
              partitionOtherDimension: List[Int],
              worker:aggregator):  aggregator= {
    sum_weights+=worker.sum_weights
    local_map_partition=local_map_partition++worker.local_map_partition
    local_blockss=local_blockss ++ worker.local_blockss
    cluster_partition= cluster_partition ++ (worker.cluster_partition.map(_._1) zip List.fill(worker.cluster_partition.size)(0))
    
    JointWorkers(worker)
    if(sum_weights==N)
    {
      val local_line_partition=local_map_partition.map(e => {
        (e._1, e._2)
      }).distinct
      val map_cluster_Partition = map_local_global_partition(cluster_partition.map(_._2).toList,
        map_partition = local_line_partition.toList)

      val globalLinePartition = global_line_partition(workerResultsCompact = local_map_partition.toList, map_cluster_Partition)
      val col_partition = globalLinePartition.reduce(_ ++ _).sortBy(_._1).map(_._2)
      val global_NIW_s = global_NIW_col(
        map_localPart_globalPart = map_cluster_Partition,
        countRow = partitionOtherDimension.max,
        countCol = cluster_partition.map(_._2).max,
        BlockSufficientStatistics =local_blockss.sortBy(_._1).map(_._2).toList)
      val local_Col_partition = globalLinePartition.map(e => {
        e.sortBy(_._1).map(_._2)
      })
      require(col_partition.size==N,s"error ${col_partition.size}")
      result=(col_partition, global_NIW_s,local_Col_partition)
    }

    this
  }

  /**
   * Ds: runRow execute streaming DPMM on the rows result of workers  (clustering of local rows clusters)
   * Input : partitionOtherDimension and worker
   * Output : aggregator
   * */
  def runRow(
              partitionOtherDimension: List[Int],
              worker: aggregator): aggregator = {
    sum_weights += worker.sum_weights
    local_map_partition = local_map_partition ++ worker.local_map_partition
    local_blockss = local_blockss ++ worker.local_blockss
    cluster_partition = cluster_partition ++ (worker.cluster_partition.map(_._1) zip List.fill(worker.cluster_partition.size)(0))

    JointWorkers(worker)
    if (sum_weights == N) {
      val local_line_partition = local_map_partition.map(e => {
        (e._1, e._2)
      }).distinct
      val map_cluster_Partition = map_local_global_partition(cluster_partition.map(_._2).toList,
        map_partition = local_line_partition.toList)

      val globalLinePartition = global_line_partition(workerResultsCompact = local_map_partition.toList, map_cluster_Partition)
      val row_partition = globalLinePartition.reduce(_ ++ _).sortBy(_._1).map(_._2)
      val global_NIW_s = global_NIW_row(
        map_localPart_globalPart = map_cluster_Partition,
        countRow = cluster_partition.map(_._2).max,
        countCol = partitionOtherDimension.max,
        BlockSufficientStatistics = local_blockss.sortBy(_._1).map(_._2).toList)
      val local_Row_partition = globalLinePartition.map(e => {
        e.sortBy(_._1).map(_._2)
      })
      require(row_partition.size == N, s"error ${row_partition.size}")
      result = (row_partition, global_NIW_s, local_Row_partition)
    }

    this
  }
}
