package Coclustering.DisNPLBM

import Coclustering.Common.NormalInverseWishart
import Coclustering.Common.ProbabilisticTools._
import Coclustering.Common.Tools.partitionToOrderedCount
import breeze.linalg.{DenseMatrix, DenseVector, sum}
import breeze.numerics.log
import scala.collection.mutable.ListBuffer

class AggregatorCol(actualAlpha: Double,
                    prior: NormalInverseWishart,
                    sufficientStatisticByRow: List[((DenseVector[Double], DenseMatrix[Double], Int), Int)],
                    mapPartition:ListBuffer[(Int, Int, Int)],
                    sufficientStatisticsByBlock:ListBuffer[(Int, List[List[(DenseVector[Double], DenseMatrix[Double], Int)]])],
                    N:Int,
                    workerId:Int,
                    beta:Double
                   ) extends Serializable {

  val id: Int = workerId
  private var localMapPartition = mapPartition
  private var sSBylocalBlock = sufficientStatisticsByBlock
  val weights: List[Int] = sufficientStatisticByRow.map(e => e._1._3)
  private var sumWeights:Int = weights.sum
  val means: List[DenseVector[Double]] = sufficientStatisticByRow.map(e => e._1._1)
  val squaredSums: List[DenseMatrix[Double]] = sufficientStatisticByRow.map(e => e._1._2)
  val data: List[((DenseVector[Double], DenseMatrix[Double], Int), Int)] = sufficientStatisticByRow
  var localCluster = data.map(_._2)
  var clusterPartition: ListBuffer[(Int, Int)] = List.fill(means.size)((id,0)).to[ListBuffer]
  val d: Int = means.head.length
  var localNIWParams: ListBuffer[NormalInverseWishart] = sufficientStatisticByRow.map(e=>{(e._2, prior.updateFromSufficientStatistics(weight = e._1._3,
                                                                                                                                      mean = e._1._1,
                                                                                                                                      SquaredSum = e._1._2))}).sortBy(_._1).map(_._2).to[ListBuffer]

  var result:(List[Int], ListBuffer[ListBuffer[NormalInverseWishart]], List[List[Int]]) = (List.fill(0)(0), new ListBuffer[ListBuffer[NormalInverseWishart]](), List.fill(1)(List.fill(0)(0)))


  /**
   * Ds: run computes line clusters in the workers using sufficient statistics by rows
   * Output : aggregator
   * */

  def run(): AggregatorCol = {
    clusterPartition = sufficientStatisticByRow.map(e=>{(id,e._2)}).to[ListBuffer]
    this
  }


  private def priorPredictive(weight: Int, mean: DenseVector[Double], squaredSum: DenseMatrix[Double]): Double = {
    prior.priorPredictiveFromSufficientStatistics(weight, mean, squaredSum)
  }

  private def computeClusterMembershipProbabilities(weight: Int, mean: DenseVector[Double], squaredSum: DenseMatrix[Double]): List[Double] = {
    val d: Int = mean.length
    localNIWParams.indices.par.map(k => {
      (k,
        localNIWParams(k).priorPredictiveFromSufficientStatistics(weight, mean, squaredSum)
          + log(localNIWParams(k).nu - d)
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
    if (newPartition == localNIWParams.length) {
      val newNIWparam = this.prior.updateFromSufficientStatistics(weight, mean, squaredSum)
      localNIWParams = localNIWParams ++ ListBuffer(newNIWparam)
    } else {
      val updatedNIWParams = localNIWParams(newPartition).updateFromSufficientStatistics(
        weight, mean, squaredSum
      )
      localNIWParams.update(newPartition, updatedNIWParams)
    }
    val local_id = clusterPartition(index)._1
    clusterPartition = clusterPartition.updated(index, (local_id, newPartition))
  }

    private def JointWorkers(worker: AggregatorCol): ListBuffer[(Int, Int)] = {
    worker.localNIWParams.indices.foreach(i => {
      val NIW = worker.localNIWParams(i)
      val newPartition = drawMembership(weight = NIW.nu, mean = NIW.mu, squaredSum = NIW.psi)
      addElementToCluster(weight = NIW.nu, mean = NIW.mu, squaredSum = NIW.psi, newPartition = newPartition, index = clusterPartition.size - worker.clusterPartition.size + i)
    })
    clusterPartition
  }
  var mapClusterPartition:List[(Int,Int,Int)]=List((0,0,0))

  /**
   * Ds: runRow execute streaming DPMM on the rows result of workers  (clustering of local rows clusters)
   * Input : partitionOtherDimension and worker
   * Output : aggregator
   * */
  def runRow(partitionOtherDimension: List[Int], worker: AggregatorCol): AggregatorCol = {
    sumWeights += worker.sumWeights
    localMapPartition = localMapPartition ++ worker.localMapPartition
    sSBylocalBlock = sSBylocalBlock ++ worker.sSBylocalBlock
    clusterPartition = clusterPartition ++ (worker.clusterPartition.map(_._1) zip List.fill(worker.clusterPartition.size)(0))
    localCluster=localCluster ++ worker.localCluster

    JointWorkers(worker)

    if (sumWeights == N) {
        val localRowPartition = localMapPartition.map(e => {(e._1, e._2)}).distinct
        mapClusterPartition = mapLocalGlobalPartition(clusterPartition = clusterPartition.toList,
                                                      localCluster = localCluster)
        val globalRowPartition = getGlobalRowPartition(workerResult = localMapPartition.toList,
                                                     masterResult = clusterPartition.toList,
                                                     localCluster = localCluster)
        val rowPartition = globalRowPartition.reduce(_ ++ _).sortBy(_._1).par.map(_._2).toList
        val globalNIWs = global_NIW_row(mapLocalPartitionWithglobalPartition = mapClusterPartition,
                                        countRow = clusterPartition.map(_._2).max,
                                        countCol = partitionOtherDimension.max,
                                        BlockSufficientStatistics = sSBylocalBlock.sortBy(_._1).map(_._2).toList,
                                        partitionOtherDimension = partitionOtherDimension,prior = prior)
        val localRowMembership = globalRowPartition.map(e => {e.sortBy(_._1).map(_._2)})

        require(localRowMembership.map(_.size).sum==N, s" global line size ${localRowMembership.map(_.size).sum}")
        require(rowPartition.size == N, s"error ${rowPartition.size}")
        result = (rowPartition, globalNIWs, localRowMembership)
    }

    this
  }

  def runCol(rowPartition: List[Int],
             globalNIW: ListBuffer[ListBuffer[NormalInverseWishart]],
             colPartition:List[Int],
             mapLocalPartitionWithglobalPartition : List[(Int, Int, Int)]): (List[Int], List[Int], ListBuffer[ListBuffer[NormalInverseWishart]]) = {

    var localGlobalNIW = globalNIW
    var blocks: List[List[List[(DenseVector[Double], DenseMatrix[Double],Int)]]] = sSBylocalBlock.sortBy(_._1).map(_._2).toList
    val P: Int = colPartition.size
    var localColPartition: List[Int] = colPartition
    var countColCluster: ListBuffer[Int] = partitionToOrderedCount(localColPartition).to[ListBuffer]

    def computeClusterMembershipProbabilities(data: List[(Int, (DenseVector[Double], DenseMatrix[Double], Int))],
                                              countColCluster: ListBuffer[Int],
                                              NIW: ListBuffer[ListBuffer[NormalInverseWishart]]):
    List[Double] = {val xByRow =data.sortBy(_._1).map(_._2)
                    NIW.indices.par.map(l => {(l, NIW.head.indices.map(k => {NIW(l)(k).priorPredictiveFromSufficientStatistics(weight = xByRow(k)._3,
                                                                                                                               mean = xByRow(k)._1 ,SquaredSum = xByRow(k)._2)}).sum + log(countColCluster(l)))}
                    ).toList.sortBy(_._1).map(_._2)}

    def priorPredictive(data: List[(Int, (DenseVector[Double], DenseMatrix[Double], Int))]):Double={
      data.par.map(e=>this.prior.priorPredictiveFromSufficientStatistics(weight = e._2._3,
        mean = e._2._1,SquaredSum = e._2._2)).sum
    }

    def removeSufficientStatisticsFromRowCluster(data: List[(Int, (DenseVector[Double], DenseMatrix[Double], Int))],
                                                 countColCluster:ListBuffer[Int],
                                                 currentPartition: Int): Unit = {
      if (countColCluster(currentPartition) == 1) {
        countColCluster.remove(currentPartition)
        localGlobalNIW.remove(currentPartition)
        localColPartition = localColPartition.map(c => {
          if (c > currentPartition) {c - 1} else c})
         } else {
        countColCluster.update(currentPartition, countColCluster.apply(currentPartition) - 1)
        data.par.foreach(element => { localGlobalNIW(currentPartition)(element._1).removeFromSufficientStatistics(weight = element._2._3,
                                                                                                                  mean = element._2._1, SquaredSum = element._2._2)})
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
                                                          mean =dataInRow._1 ,
                                                          SquaredSum =dataInRow._2 ))}).toList.sortBy(_._1).map(_._2).to[ListBuffer]
            localGlobalNIW.append(newCluster)
      } else {
        countColCluster.update(newPartition, countColCluster.apply(newPartition) + 1)
        data.par.foreach(e => {
           val k = e._1
           val dataInCol = e._2
           localGlobalNIW(newPartition).update(k,
           localGlobalNIW(newPartition)(k).updateFromSufficientStatistics(weight = dataInCol._3,
           mean =dataInCol._1 ,SquaredSum =dataInCol._2 ))
        })
      }
    }

    /**
     * Ds: computeGlobalNIW computes global blocks' NIWs after col clustering using workers blocks' sufficient statistics
     * Input: countRow : Int = number of row clusters,
     * countCol :Int = number of columns clusters
     * BlockSufficientStatistics : List(List(List((mean,covariance,weight)))) the first list represent the workers,
     * the second one represent column wise matrix sufficient statistics
     * Output : Global column wise NIWs matrix
     * */

    def computeGlobalNIW(rowPartition: List[Int], colPartitions: List[Int], BlockSufficientStatistics: List[List[List[(DenseVector[Double], DenseMatrix[Double], Int)]]]):

      ListBuffer[ListBuffer[NormalInverseWishart]] = {
        var result: ListBuffer[ListBuffer[NormalInverseWishart]] = ListBuffer()
        val indexColInColCluster=colPartitions.zipWithIndex.groupBy(_._1)
        for (i <- 0 to colPartitions.max) {
          val NIWsByRow: ListBuffer[NormalInverseWishart] = ListBuffer()
            for (j <- 0 to rowPartition.max) {
              val mapSufficientStatistics = mapLocalPartitionWithglobalPartition.filter(_._3 == j).map(e => {(e._1, e._2)})
              val colClusters = indexColInColCluster.filter(_._1 == i).head._2.map(_._2)
              val SufficientStatistics_j = mapSufficientStatistics.indices.map(index_row => {
              val tmp = mapSufficientStatistics(index_row)
              val t1 = colClusters.par.map(index_col => {BlockSufficientStatistics(tmp._1)(index_col)(tmp._2)}).toList
              t1}).toList
              val flattenSufficientStatistics_j = SufficientStatistics_j.flatten
              val meansPerCluster = flattenSufficientStatistics_j.map(_._1)
              val weightsPerCluster = flattenSufficientStatistics_j.map(_._3)
              val aggregatedMeans: DenseVector[Double] = aggregateMeans(meansPerCluster, weightsPerCluster)
              val squaredSumsPerCluster: List[DenseMatrix[Double]] = flattenSufficientStatistics_j.map(_._2)
              val aggregatedsS: DenseMatrix[Double] = aggregateSquaredSums(sS = squaredSumsPerCluster,
                                                                           ms = meansPerCluster,
                                                                           ws = weightsPerCluster,
                                                                           aggMean = aggregatedMeans)
              NIWsByRow.append(prior.updateFromSufficientStatistics(weight = sum(weightsPerCluster),
                                                                   mean = aggregatedMeans,
                                                                   SquaredSum = aggregatedsS))}
            result.append(NIWsByRow)}
          result
      }
      for (i <- 0 until P){
        val currentData= blocks.indices.par.map(k=>{
           (blocks(k)(i).zipWithIndex).map(e=>{
                (e._1, mapLocalPartitionWithglobalPartition.filter(ele=>{
                                                                        ele._1==k && ele._2==e._2}).head._3)})}).flatten.toList.groupBy(_._2).values.par.map(eles=>{ val ss=eles.map(_._1)
                                                                                                                                                                     val meansPerCluster = ss.map(_._1)
                                                                                                                                                                     val weightsPerCluster =ss.map(_._3)
                                                                                                                                                                     val aggregatedMeans: DenseVector[Double] = aggregateMeans(meansPerCluster, weightsPerCluster)
                                                                                                                                                                     val squaredSumsPerCluster: List[DenseMatrix[Double]] = ss.map(_._2)
                                                                                                                                                                     val aggregatedsS: DenseMatrix[Double] = aggregateSquaredSums(sS = squaredSumsPerCluster,
                                                                                                                                                                                                                                  ms = meansPerCluster,
                                                                                                                                                                                                                                  ws = weightsPerCluster,
                                                                                                                                                                                                                                  aggMean = aggregatedMeans)
        (eles.head._2,(aggregatedMeans,aggregatedsS,sum(weightsPerCluster)))}).toList
        val currentPartition = localColPartition(i)
        removeSufficientStatisticsFromRowCluster(currentData, countColCluster = countColCluster,currentPartition)
        val newPartition = drawSufficientStatisticsMembership(data = currentData,
                                                              countColCluster=countColCluster,
                                                              NIW=localGlobalNIW,
                                                              beta=beta)
        localColPartition = localColPartition.updated(i, newPartition)
        addSufficientStatisticsToRowCluster(data = currentData,
                                            newPartition=newPartition)}

        localGlobalNIW=computeGlobalNIW(rowPartition = rowPartition,
                                        colPartitions = localColPartition,
                                        BlockSufficientStatistics = blocks)
        (rowPartition, localColPartition, localGlobalNIW)
  }
}