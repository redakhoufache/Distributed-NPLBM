package DAVID.FunDisNPLBMRow

import DAVID.Common.NormalInverseWishart
import DAVID.Common.ProbabilisticTools._
import DAVID.Common.Tools.partitionToOrderedCount
import breeze.linalg.{DenseMatrix, DenseVector, sum}
import breeze.numerics.log
import scala.collection.mutable.ListBuffer

class AggregatorCol(actualAlpha: Double,
                    prior: NormalInverseWishart,
                    line_sufficientStatistic: List[((DenseVector[Double], DenseMatrix[Double], Int),Int)],
                    map_partition:ListBuffer[(Int,Int,Int)],
                    blockss:ListBuffer[(Int,List[List[(DenseVector[Double], DenseMatrix[Double],Int)]])],
                    N:Int,
                    worker_id:Int,
                    beta:Double
                   ) extends Serializable {
  val id: Int =worker_id
  private var local_map_partition=map_partition
  private var local_blockss=blockss
  val weights: List[Int] = line_sufficientStatistic.map(e => e._1._3)
  private var sum_weights:Int=weights.sum
  val means: List[DenseVector[Double]] = line_sufficientStatistic.map(e => e._1._1)
  val squaredSums: List[DenseMatrix[Double]] = line_sufficientStatistic.map(e => e._1._2)
  val Data: List[((DenseVector[Double], DenseMatrix[Double], Int),Int)] = line_sufficientStatistic
  var local_k_cluster=Data.map(_._2)
  var cluster_partition: ListBuffer[(Int,Int)] = List.fill(means.size)((id,0)).to[ListBuffer]
  var NIWParams_local: ListBuffer[NormalInverseWishart] = line_sufficientStatistic.map(e=>{
    (e._2, prior.updateFromSufficientStatistics(
      weight = e._1._3,
      mean = e._1._1,
      SquaredSum = e._1._2
    ))
  }).sortBy(_._1).map(_._2).to[ListBuffer]
  val d: Int = means.head.length
  /*============================================================================================*/
  var result:(List[Int], ListBuffer[ListBuffer[NormalInverseWishart]], List[List[Int]])=(List.fill(0)(0),
    new ListBuffer[ListBuffer[NormalInverseWishart]](),List.fill(1)(List.fill(0)(0)))


  /**
   * Ds: run computes line clusters in the workers using lines' sufficient statistics
   * Output : aggregator
   * */
  def run(): AggregatorCol = {
    cluster_partition = line_sufficientStatistic.map(e=>{(id,e._2)}).to[ListBuffer]
    this
  }


  private def priorPredictive(weight: Int, mean: DenseVector[Double], squaredSum: DenseMatrix[Double]): Double = {
    prior.priorPredictiveFromSufficientStatistics(weight, mean, squaredSum)
  }

  private def computeClusterMembershipProbabilities(weight: Int, mean: DenseVector[Double], squaredSum: DenseMatrix[Double]): List[Double] = {
    val d: Int = mean.length
    NIWParams_local.indices.par.map(k => {
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
   * Ds: JointWorkers clusters the DPMM's result of an a aggregatorCol into an other one
   * Input : aggregator
   * Output : cluster line membership (DPMM result)
   * */
  private def JointWorkers(worker: AggregatorCol): ListBuffer[(Int, Int)] = {
    worker.NIWParams_local.indices.foreach(i => {
      val NIW = worker.NIWParams_local(i)
      val newPartition = drawMembership(weight = NIW.nu, mean = NIW.mu, squaredSum = NIW.psi)
      addElementToCluster(weight = NIW.nu, mean = NIW.mu, squaredSum = NIW.psi, newPartition = newPartition, index = cluster_partition.size - worker.cluster_partition.size + i)
    })
    cluster_partition
  }
  var map_cluster_Partition:List[(Int,Int,Int)]=List((0,0,0))
  /**
   * Ds: runRow execute streaming DPMM on the rows result of workers  (clustering of local rows clusters)
   * Input : partitionOtherDimension and worker
   * Output : aggregator
   * */
  def runRow(
              partitionOtherDimension: List[Int],
              worker: AggregatorCol): AggregatorCol = {
    sum_weights += worker.sum_weights
    local_map_partition = local_map_partition ++ worker.local_map_partition
    local_blockss = local_blockss ++ worker.local_blockss
    cluster_partition = cluster_partition ++ (worker.cluster_partition.map(_._1) zip List.fill(worker.cluster_partition.size)(0))
    local_k_cluster=local_k_cluster++worker.local_k_cluster
    JointWorkers(worker)
    if (sum_weights == N) {
      val local_line_partition = local_map_partition.map(e => {
        (e._1, e._2)
      }).distinct
      map_cluster_Partition = map_local_global_partition(cluster_partition = cluster_partition.toList,
        local_k_cluster = local_k_cluster)
      val globalLinePartition = global_line_partition(worker_result = local_map_partition.toList,
        master_result = cluster_partition.toList,local_k_cluster = local_k_cluster)
      val row_partition = globalLinePartition.reduce(_ ++ _).sortBy(_._1).par.map(_._2).toList
      val global_NIW_s = global_NIW_row(
        map_localPart_globalPart = map_cluster_Partition,
        countRow = cluster_partition.map(_._2).max,
        countCol = partitionOtherDimension.max,
        BlockSufficientStatistics = local_blockss.sortBy(_._1).map(_._2).toList,
        partitionOtherDimension = partitionOtherDimension,prior = prior)
      val local_Row_partition = globalLinePartition.map(e => {
        e.sortBy(_._1).map(_._2)
      })
      require(local_Row_partition.map(_.size).sum==N, s" global line size ${local_Row_partition.map(_.size).sum}")
      require(row_partition.size == N, s"error ${row_partition.size}")
      result = (row_partition, global_NIW_s, local_Row_partition)
    }

    this
  }


  /**
   * Ds: runCol execute streaming DPMM on the columns result of workers  (clustering of local columns clusters)
   * Input : partitionOtherDimension and worker
   * Output : aggregatorCol
   * */
  def runCol(
              row_partition: List[Int],
              global_NIW: ListBuffer[ListBuffer[NormalInverseWishart]],
              col_partition:List[Int],
              map_localPart_globalPart:List[(Int,Int,Int)]
            ): (List[Int],List[Int],ListBuffer[ListBuffer[NormalInverseWishart]]) = {
    /*--------------------------------------------------------------------------------------------------*/
    var local_global_NIW=global_NIW
    var blocks: List[List[List[(DenseVector[Double], DenseMatrix[Double],Int)]]] = local_blockss.sortBy(_._1).map(_._2).toList
    val P: Int = col_partition.size
    var local_col_partition: List[Int] = col_partition
    var countColCluster: ListBuffer[Int] = partitionToOrderedCount(local_col_partition).to[ListBuffer]
    /*--------------------------------------------------------------------------------------------------*/
    def computeClusterMembershipProbabilities(data: List[(Int, (DenseVector[Double], DenseMatrix[Double], Int))],
                                              countColCluster: ListBuffer[Int],
                                              NIW: ListBuffer[ListBuffer[NormalInverseWishart]]):
    List[Double]={
      val xByRow =data.sortBy(_._1).map(_._2)
      NIW.indices.par.map(l => {
        (l, NIW.head.indices.map(k => {
          NIW(l)(k).priorPredictiveFromSufficientStatistics(weight = xByRow(k)._3,
            mean =xByRow(k)._1 ,SquaredSum = xByRow(k)._2)
        }).sum + log(countColCluster(l)))
      }).toList.sortBy(_._1).map(_._2)
    }
    def priorPredictive(data: List[(Int, (DenseVector[Double], DenseMatrix[Double], Int))]):Double={
      data.par.map(e=>this.prior.priorPredictiveFromSufficientStatistics(weight = e._2._3,
        mean = e._2._1,SquaredSum = e._2._2)).sum
    }
    def removeSufficientStatisticsFromRowCluster(data: List[(Int, (DenseVector[Double], DenseMatrix[Double], Int))],
                                                 countColCluster:ListBuffer[Int],
                                                 currentPartition: Int): Unit = {
      if (countColCluster(currentPartition) == 1) {
        countColCluster.remove(currentPartition)
        local_global_NIW.remove(currentPartition)
        local_col_partition = local_col_partition.map(c => {
          if (c > currentPartition) {
            c - 1
          } else c
        })
      } else {
        countColCluster.update(currentPartition, countColCluster.apply(currentPartition) - 1)
        data.par.foreach(element => {
          local_global_NIW(currentPartition)(element._1).removeFromSufficientStatistics(weight = element._2._3,
            mean = element._2._1, SquaredSum = element._2._2)
        })
      }
    }
    def drawSufficientStatisticsMembership(data: List[(Int, (DenseVector[Double], DenseMatrix[Double], Int))],
                                           countColCluster:ListBuffer[Int],
                                           NIW: ListBuffer[ListBuffer[NormalInverseWishart]],beta:Double):Int={

      val probPartition = computeClusterMembershipProbabilities(data, countColCluster, NIW)
      val posteriorPredictiveXi = priorPredictive(data)
      val probs = probPartition :+ (posteriorPredictiveXi + log(beta))
      val normalizedProbs = normalizeLogProbability(probs)
      sample(normalizedProbs)
    }
    def addSufficientStatisticsToRowCluster(data: List[(Int, (DenseVector[Double], DenseMatrix[Double], Int))],
                                            newPartition:Int):Unit={
      if (newPartition == countColCluster.length) {
        countColCluster.append(1)
        val newCluster = data.par.map(e => {
          val k = e._1
          val dataInRow = e._2
          (k, this.prior.updateFromSufficientStatistics(weight = dataInRow._3,
            mean =dataInRow._1 ,SquaredSum =dataInRow._2 ))
        }).toList.sortBy(_._1).map(_._2).to[ListBuffer]
        local_global_NIW.append(newCluster)
      } else {
        countColCluster.update(newPartition, countColCluster.apply(newPartition) + 1)
        data.par.foreach(e => {
          val k = e._1
          val dataInCol = e._2
          local_global_NIW(newPartition).update(k,
            local_global_NIW(newPartition)(k).updateFromSufficientStatistics(weight = dataInCol._3,
              mean =dataInCol._1 ,SquaredSum =dataInCol._2 ))
        })
      }

    }

    /**
     * Ds: global_NIW_col computes global blocks' NIWs after col clustering using workers blocks' sufficient statistics
     * Input: countRow : Int = number of row clusters,
     * countCol :Int = number of columns clusters
     * BlockSufficientStatistics : List(List(List((mean,covariance,weight)))) the first list represent the workers,
     * the second one represent column wise matrix sufficient statistics
     * Output : Global column wise NIWs matrix
     * */
    def global_NIW_col(
                        rowParations: List[Int],
                        colParations: List[Int],
                        BlockSufficientStatistics: List[
                          List[List[(DenseVector[Double], DenseMatrix[Double], Int)]]]):
    ListBuffer[ListBuffer[NormalInverseWishart]] = {
      var result: ListBuffer[ListBuffer[NormalInverseWishart]] = ListBuffer()
      val index_columns_in_colCluster=colParations.zipWithIndex.groupBy(_._1)
      for (i <- 0 to colParations.max) {
        val row_NIWs: ListBuffer[NormalInverseWishart] = ListBuffer()
        for (j <- 0 to rowParations.max) {
          /*map_SufficientStatistics_j is List[(worker_id,local_row_partition)]*/
          val map_SufficientStatistics_j = map_localPart_globalPart.filter(_._3 == j).map(e => {
            (e._1, e._2)
          })
          val list_col_clusters = index_columns_in_colCluster.filter(_._1 == i).head._2.map(_._2)
          val SufficientStatistics_j = map_SufficientStatistics_j.indices.map(index_row => {
            val tmp = map_SufficientStatistics_j(index_row)
            val t1 = list_col_clusters.par.map(index_col => {
              BlockSufficientStatistics(tmp._1)(index_col)(tmp._2)
            }).toList
            t1
          }).toList
          val flatten_SufficientStatistics_j = SufficientStatistics_j.flatten
          val meansPerCluster = flatten_SufficientStatistics_j.map(_._1)
          val weightsPerCluster = flatten_SufficientStatistics_j.map(_._3)
          val aggregatedMeans: DenseVector[Double] = aggregateMeans(meansPerCluster, weightsPerCluster)
          val squaredSumsPerCluster: List[DenseMatrix[Double]] = flatten_SufficientStatistics_j.map(_._2)
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
    /*--------------------------------------------------------------------------------------------------------------*/
    for (i <- 0 until P){
      /*aggregate col sufficientStatistics according row partitions (m+1)*/
      val currentData= blocks.indices.par.
        map(k=>{(blocks(k)(i).zipWithIndex).map(e=>{
          (e._1,map_localPart_globalPart.filter(ele=>{ele._1==k && ele._2==e._2}).head._3)
        })}).flatten.toList.groupBy(_._2).values.par.map(eles=>{
        val ss=eles.map(_._1)
        val meansPerCluster = ss.map(_._1)
        val weightsPerCluster =ss.map(_._3)
        val aggregatedMeans: DenseVector[Double] = aggregateMeans(meansPerCluster, weightsPerCluster)
        val squaredSumsPerCluster: List[DenseMatrix[Double]] = ss.map(_._2)
        val aggregatedsS: DenseMatrix[Double] = aggregateSquaredSums(
          sS = squaredSumsPerCluster,
          ms = meansPerCluster,
          ws = weightsPerCluster,
          aggMean = aggregatedMeans
        )
        (eles.head._2,(aggregatedMeans,aggregatedsS,sum(weightsPerCluster)))
      }).toList
      val currentPartition = local_col_partition(i)
      removeSufficientStatisticsFromRowCluster(currentData, countColCluster = countColCluster,currentPartition)
      val newPartition = drawSufficientStatisticsMembership(data = currentData,
        countColCluster=countColCluster, NIW=local_global_NIW, beta=beta)
      local_col_partition = local_col_partition.updated(i, newPartition)
      addSufficientStatisticsToRowCluster(data = currentData, newPartition=newPartition)
    }
    local_global_NIW=global_NIW_col(rowParations = row_partition,colParations = local_col_partition,BlockSufficientStatistics = blocks)
    (row_partition,local_col_partition,local_global_NIW)
  }
}