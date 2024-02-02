package Coclustering.Common

import Coclustering.Common.Tools._
import breeze.linalg.{*, DenseMatrix, DenseVector, diag, inv, max, sum}
import breeze.numerics.{exp, log, sqrt}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.writePretty

import java.io.{File, PrintWriter}
import scala.collection.immutable.SortedMap
import scala.collection.mutable.ListBuffer
object ProbabilisticTools extends java.io.Serializable {

  def computeAndSaveglobalMeanPrecision(datasetPath: String, datasetName: String, dataList: List[List[DenseVector[Double]]]): Unit= {
    implicit val formats: DefaultFormats.type = DefaultFormats

    val dataFlattened = dataList.reduce(_ ++ _)
    val globalMean = ProbabilisticTools.meanListDV(dataFlattened)
    val globalVariance = ProbabilisticTools.covariance(dataFlattened, globalMean)
    val globalPrecision = inv(globalVariance)
    val globalMeanList: List[Double] = globalMean.toArray.toList
    val globalPrecisionList: List[List[Double]] = (0 until globalPrecision.rows).map(row => globalPrecision(row, ::).inner.toArray.toList).toList

    val mean = writePretty(globalMeanList)
    val f = new File(s"$datasetPath/data/global_mean_${datasetName.split('.')(0)}.json")
    val w = new PrintWriter(f)
    w.write(mean)
    w.close()

    val precision = writePretty(globalPrecisionList)
    val g = new File(s"$datasetPath/data/global_precision_${datasetName.split('.')(0)}.json")
    val z = new PrintWriter(g)
    z.write(precision)
    z.close()
  }

  def covariance(X: DenseVector[Double],Y: DenseVector[Double]): Double = {
    sum( (X- meanDV(X)) *:* (Y- meanDV(Y)) ) / (Y.length-1)
  }
  def covariance(X: List[DenseVector[Double]], mode: DenseVector[Double], constraint: String = "none"): DenseMatrix[Double] = {

    require(List("none","independant").contains(constraint))
    require(mode.length==X.head.length)
    val XMat: DenseMatrix[Double] = DenseMatrix(X.toArray:_*)
    val p = XMat.cols
    val covmat = constraint match {
      case "independant" => DenseMatrix.tabulate[Double](p,p){(i, j) => if(i == j) covariance(XMat(::,i),XMat(::,i)) else 0D}
      case _ => {
        val modeMat: DenseMatrix[Double] = DenseMatrix.ones[Double](X.length,1) * mode.t
        val XMatCentered: DenseMatrix[Double] = XMat - modeMat
        XMatCentered.t * XMatCentered
      }/ (X.length.toDouble-1)
    }

    roundMat(covmat,8)
  }
  def meanDV(X: DenseVector[Double]): Double = {
    sum(X)/X.length
  }

  def meanListDV(X: List[DenseVector[Double]]): DenseVector[Double] = {
    require(X.nonEmpty)
    X.reduce(_+_) / X.length.toDouble
  }
  def sample(probabilities: List[Double]): Int = {
    val dist = probabilities.indices zip probabilities
    val threshold = scala.util.Random.nextDouble
    val iterator = dist.iterator
    var accumulator = 0.0
    while (iterator.hasNext) {
      val (cluster, clusterProb) = iterator.next
      accumulator += clusterProb
      if (accumulator >= threshold)
        return cluster
    }
    sys.error("Error")
  }

  def sample(nCategory: Int, weight: List[Double] = Nil): Int = {
    val finalWeight = if(weight==Nil){
      List.fill(nCategory)(1D/nCategory.toDouble)
    } else {
      weight
    }
    sample(finalWeight)
  }

  def sampleWithSeed(probabilities: List[Double], seed: Long ): Int = {
    val dist = probabilities.indices zip probabilities
    val r = new scala.util.Random(seed)
    val threshold = r.nextDouble
    val iterator = dist.iterator
    var accumulator = 0.0
    while (iterator.hasNext) {
      val (cluster, clusterProb) = iterator.next
      accumulator += clusterProb
      if (accumulator >= threshold)
        return cluster
    }
    sys.error("Error")
  }

  def scale(data: List[List[DenseVector[Double]]]): List[List[DenseVector[Double]]] = {
    data.map(column => {
      val mode = meanListDV(column)
      val cov = covariance(column,mode,"independant")
      val std = sqrt(diag(cov))
      column.map(_ /:/ std)
    })
  }

  def weight(data: List[List[DenseVector[Double]]], weight: DenseVector[Double]): List[List[DenseVector[Double]]] = {
    data.map(column => {
      column.map(_ *:* weight)
    })
  }

  def logSumExp(X: List[Double]): Double ={
    val maxValue = max(X)
    maxValue + log(sum(X.map(x => exp(x-maxValue))))
  }

  def normalizeLogProbability(probs: List[Double]): List[Double] = {
    val LSE = ProbabilisticTools.logSumExp(probs)
    probs.map(e => exp(e - LSE))
  }

  def unitCovFunc(fullCovarianceMatrix:Boolean):
  DenseVector[Double] => DenseMatrix[Double] = (x: DenseVector[Double]) => {
    if(fullCovarianceMatrix){
      x * x.t
    } else {
      diag(x *:* x)
    }
  }
  def aggregateMeans(ms: List[DenseVector[Double]], ws: List[Int]): DenseVector[Double] = {
    val sums = (ms zip ws).par.map(e => e._2.toDouble * e._1).reduce(_ + _) / sum(ws).toDouble
    sums
  }

  def aggregateSquaredSums(sS: List[DenseMatrix[Double]],
                                   ms: List[DenseVector[Double]],
                                   ws: List[Int],
                                   aggMean: DenseVector[Double]): DenseMatrix[Double] = {
    val aggM: DenseMatrix[Double] =
      sum(sS) + (ms zip ws).par.map(e => (e._1 * e._1.t) * e._2.toDouble).reduce(_ + _) - sum(ws).toDouble * aggMean * aggMean.t
    aggM
  }

  def mapLocalGlobalPartition(clusterPartition: List[(Int, Int)],
                              localCluster: List[Int]):
  List[(Int, Int, Int)] = {
    (clusterPartition zip localCluster).map(e => (e._1._1, e._2, e._1._2))
  }

  /**
   * Ds: global_line_partition computes the global line (row or column) membership using worker membership
   * Input:worker_result List(worker_id,local_membership,line_index) of size number_of_lines
   * master_result List(worker_id,global_membership) of size number_of_clusters
   * Output: List(List(index,global_membership,local_membership)) for size (number_of_clusters * number_of_lines)
   * */
  def getGlobalRowPartition(workerResult: List[(Int, Int, Int)],
                            masterResult: List[(Int, Int)],
                            localCluster: List[Int]): List[List[(Int, Int, Int)]] = {
    val localGlobalMembership = (masterResult zip localCluster).map(e => (e._1._1, e._2, e._1._2))
    val result = workerResult.map(line => {
      val globalMembership = localGlobalMembership.filter(x => {
        x._1 == line._1 && x._2 == line._2
      }).head._3
      (line._1, line._3, globalMembership, line._2)
    })
    val tmp = SortedMap(result.groupBy(_._1).toSeq: _*).values.toList
    tmp.map(_.map(e => {
      (e._2, e._3, e._4)
    }))
  }

  /**
   * Ds: getGlobalNIWRowClust computes global blocks' NIWs after row clustering using workers blocks' sufficient statistics
   * Input: mapLocalWithglobalPartition List[(worker_id,local_cluster_id,global_cluster_id)]
   * countRow : Int = number of row clusters,
   * countCol :Int = number of columns clusters
   * BlockSufficientStatistics : List(List(List((mean,covariance,weight)))) the first list represent the workers,
   * the second one represent column wise matrix sufficient statistics
   * Output : Global column wise  NIWs matrix
   * */
   def getGlobalNIWRowClust(mapLocalPartitionWithglobalPartition: List[(Int, Int, Int)],
                            countRow: Int,
                            countCol: Int,
                            BlockSufficientStatistics: List[List[List[(DenseVector[Double], DenseMatrix[Double], Int)]]],
                            partitionOtherDimension: List[Int],
                            prior: NormalInverseWishart): ListBuffer[ListBuffer[NormalInverseWishart]] = {

    val result: ListBuffer[ListBuffer[NormalInverseWishart]] = ListBuffer()
    val indexCoInColCluster = partitionOtherDimension.zipWithIndex.groupBy(_._1)
    for (i <- 0 to countCol) {
      val NIWByRows: ListBuffer[NormalInverseWishart] = ListBuffer()
      for (j <- 0 to countRow) {
        val mapSufficientStatistics_j = mapLocalPartitionWithglobalPartition.filter(_._3 == j).map(e => {(e._1, e._2)})
        val colClusters = indexCoInColCluster.filter(_._1 == i).head._2.map(_._2)
        val sufficientStatistics_j = mapSufficientStatistics_j.indices.map(index_row => {
          val tmp = mapSufficientStatistics_j(index_row)
          val t1 = colClusters.par.map(index_col => {
            BlockSufficientStatistics(tmp._1)(index_col)(tmp._2)}).toList
          t1}).toList
        val flattenSufficientStatistics_j = sufficientStatistics_j.flatten
        val meansPerCluster = flattenSufficientStatistics_j.map(_._1)
        val weightsPerCluster = flattenSufficientStatistics_j.map(_._3)
        val aggregatedMeans: DenseVector[Double] = aggregateMeans(meansPerCluster, weightsPerCluster)
        val squaredSumsPerCluster: List[DenseMatrix[Double]] = flattenSufficientStatistics_j.map(_._2)
        val aggregatedsS: DenseMatrix[Double] = aggregateSquaredSums(sS = squaredSumsPerCluster,
                                                                     ms = meansPerCluster,
                                                                     ws = weightsPerCluster,
                                                                     aggMean = aggregatedMeans)
        NIWByRows.append(prior.updateFromSufficientStatistics(weight = sum(weightsPerCluster), mean = aggregatedMeans, SquaredSum = aggregatedsS))}
        result.append(NIWByRows)
    }
    result
  }
}
