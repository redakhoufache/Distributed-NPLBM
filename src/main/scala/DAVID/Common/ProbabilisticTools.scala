package DAVID.Common

import DAVID.Common.Tools._
import breeze.linalg.{*, DenseMatrix, DenseVector, argmax, diag, max, sum}
import breeze.numerics.{exp, log, sqrt}
import breeze.stats.distributions._
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.immutable.SortedMap
import scala.collection.mutable.ListBuffer
object ProbabilisticTools extends java.io.Serializable {

  def variance(X: DenseVector[Double]): Double = {
    covariance(X,X)
  }

  def covariance(X: DenseVector[Double],Y: DenseVector[Double]): Double = {
    sum( (X- meanDV(X)) *:* (Y- meanDV(Y)) ) / (Y.length-1)
  }

  def covarianceSpark(X: RDD[((Int, Int), Vector)],
                      modes: Map[(Int, Int), DenseVector[Double]],
                      count: Map[(Int, Int), Int]): Map[(Int, Int),  DenseMatrix[Double]] = {

    val XCentered : RDD[((Int, Int), DenseVector[Double])] = X.map(d => (d._1, DenseVector(d._2.toArray) - DenseVector(modes(d._1).toArray)))

    val internProduct = XCentered.map(row => (row._1, row._2 * row._2.t))
    val internProductSumRDD: RDD[((Int,Int), DenseMatrix[Double])] = internProduct.reduceByKey(_+_)
    val interProductSumList: List[((Int,Int),  DenseMatrix[Double])] = internProductSumRDD.collect().toList

    interProductSumList.map(c => (c._1,c._2/(count(c._1)-1).toDouble)).toMap

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

  def weightedCovariance(X: DenseVector[Double], Y: DenseVector[Double], weights: DenseVector[Double]): Double = {
    sum( weights *:* (X- meanDV(X)) *:* (Y- meanDV(Y)) ) / sum(weights)
  }

  def weightedCovariance (X: List[DenseVector[Double]],
                          weights: DenseVector[Double],
                          mode: DenseVector[Double],
                          constraint: String = "none"): DenseMatrix[Double] = {
    require(List("none","independant").contains(constraint))
    require(mode.length==X.head.length)
    require(weights.length==X.length)

    val XMat: DenseMatrix[Double] = DenseMatrix(X.toArray:_*)
    //    val p = XMat.cols
    val q = mode.length
    constraint match {
      case "independant" => DenseMatrix.tabulate[Double](q,q){(i, j) => if(i == j) weightedCovariance(XMat(::,i),XMat(::,i), weights) else 0D}
      case _ =>
        val modeMat: DenseMatrix[Double] = DenseMatrix.ones[Double](X.length,1) * mode.t
        val XMatCentered: DenseMatrix[Double] = XMat - modeMat
        val res = DenseMatrix((0 until XMatCentered.rows).par.map(i => {
          weights(i)*XMatCentered(i,::).t * XMatCentered(i,::)
        }).reduce(_+_).toArray:_*)/ sum(weights)
        res.reshape(q,q)
    }
  }

  def meanDV(X: DenseVector[Double]): Double = {
    sum(X)/X.length
  }

  def meanListDV(X: List[DenseVector[Double]]): DenseVector[Double] = {
    require(X.nonEmpty)
    X.reduce(_+_) / X.length.toDouble
  }

  def weightedMean(X: List[DenseVector[Double]], weights: DenseVector[Double]): DenseVector[Double] = {
    require(X.length == weights.length)
    val res = X.indices.par.map(i => weights(i) * X(i)).reduce(_+_) / sum(weights)
    res
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

  def clustersParametersEstimation(Data: List[DenseVector[Double]],
                                   prior: NormalInverseWishart,
                                   rowPartition: List[Int]) : List[MultivariateGaussian] = {


    (Data zip rowPartition).groupBy(_._2).values.par.map(e => {
      val dataInCluster = e.map(_._1)
      val k = e.head._2
      val sufficientStatistic = prior.update(dataInCluster)
      (k, sufficientStatistic.sample())
    }).toList.sortBy(_._1).map(_._2)

  }

  def clustersParametersEstimation(dataByCol: List[List[DenseVector[Double]]],
                                   prior: NormalInverseWishart,
                                   rowPartition: List[Int],
                                   colPartition: List[Int]) : List[List[MultivariateGaussian]] = {

    (dataByCol zip colPartition).groupBy(_._2).values.par.map(e => {
      val dataInCol = e.map(_._1)
      val l = e.head._2
      (l,
        (dataInCol.transpose zip rowPartition).groupBy(_._2).values.par.map(f => {
          val dataInBlock = f.map(_._1).reduce(_++_)
          val k = f.head._2
          val sufficientStatistic = prior.update(dataInBlock)
          (k, sufficientStatistic.sample())
        }).toList.sortBy(_._1).map(_._2))

    }).toList.sortBy(_._1).map(_._2)
  }

  def clustersParametersEstimationMC(dataByCol: List[List[DenseVector[Double]]],
                                     prior: NormalInverseWishart,
                                     rowPartitions: List[List[Int]],
                                     colPartition: List[Int])(implicit d: DummyImplicit) = {

    (dataByCol zip colPartition).groupBy(_._2).values.par.map(e => {
      val dataInCol = e.map(_._1)
      val l = e.head._2
      (l,
        (dataInCol.transpose zip rowPartitions(l)).groupBy(_._2).values.par.map(f => {
          val dataInBlock = f.map(_._1).reduce(_++_)
          val k = f.head._2
          val sufficientStatistic = prior.update(dataInBlock)
          (k, sufficientStatistic.sample())
        }).toList.sortBy(_._1).map(_._2))

    }).toList.sortBy(_._1).map(_._2)
  }

  def clustersParametersEstimationMCC(dataByCol: List[List[DenseVector[Double]]],
                                      prior: NormalInverseWishart,
                                      redundantColPartition: List[Int],
                                      correlatedColPartitions: List[List[Int]],
                                      rowPartitions: List[List[Int]]) = {

    val combinedColPartition = Tools.combineRedundantAndCorrelatedColPartitions(redundantColPartition, correlatedColPartitions)

    val rowPartitionDuplicatedPerColCluster = correlatedColPartitions.indices.map(h => {
      List.fill(correlatedColPartitions(h).distinct.length)(rowPartitions(h))
    }).reduce(_++_)

    clustersParametersEstimationMC(dataByCol, prior, rowPartitionDuplicatedPerColCluster, combinedColPartition)
  }

  
  def clustersParametersEstimationDPV(dataByCol: List[List[DenseVector[Double]]],
                                      priors: List[NormalInverseWishart],
                                      rowPartitions: List[List[Int]],
                                      colPartition: List[Int])(implicit d: DummyImplicit) = {

    dataByCol.indices.par.map(j => {
      (j,
        (dataByCol(j) zip rowPartitions(colPartition(j))).groupBy(_._2).values.par.map(f => {
          val dataInBlock = f.map(_._1)
          val k = f.head._2
          val sufficientStatistic = priors(j).update(dataInBlock)
          (k, sufficientStatistic.sample())
        }).toList.sortBy(_._1).map(_._2))

    }).toList.sortBy(_._1).map(_._2)
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

  def intToVecProb(i: Int, size:Int): List[Double] = {
    val b = mutable.Buffer.fill(size)(1e-8)
    b(i)=1D
    val sum = b.sum
    b.map(_/sum) .toList
  }

  def partitionToBelongingProbabilities(partition: List[Int], toLog:Boolean=false): List[List[Double]]={

    val K = partition.max+1
    val res = partition.indices.map(i => {
      intToVecProb(partition(i),K)
    }).toList

    if(!toLog){res} else {res.map(_.map(log(_)))}
  }


  def logSumExp(X: List[Double]): Double ={
    val maxValue = max(X)
    maxValue + log(sum(X.map(x => exp(x-maxValue))))
  }

  def logSumExp(X: DenseVector[Double]): Double ={
    val maxValue = max(X)
    maxValue + log(sum(X.map(x => exp(x-maxValue))))
  }

  def normalizeProbability(probs: List[Double]): List[Double] = {
    normalizeLogProbability(probs.map(e => log(e)))
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

  def mapDm(probBelonging: DenseMatrix[Double]): List[Int] = {
    probBelonging(*,::).map(argmax(_)).toArray.toList
  }

  def MAP(probBelonging: List[List[Double]]): List[Int] = {
    probBelonging.map(Tools.argmax)
  }


  def updateAlpha(alpha: Double, alphaPrior: Gamma, nCluster: Int, nObservations: Int): Double = {
    val shape = alphaPrior.shape
    val rate =  1D/alphaPrior.scale

    val log_x = log(new Beta(alpha + 1, nObservations).draw())
    val pi1 = shape + nCluster + 1
    val pi2 = nObservations * (rate - log_x)
    val pi = pi1/(pi1+pi2)
    val newScale = 1/(rate - log_x)

    max(if(sample(List(pi, 1-pi)) == 0){
      Gamma(shape = shape + nCluster, newScale).draw()
    } else {
      Gamma(shape = shape + nCluster - 1, newScale).draw()
    }, 1e-8)
  }

  def stickBreaking(hyperPrior: Gamma, size:Int, seed : Option[Int] = None): List[Double] = {

    val actualSeed: Int = seed match {
      case Some(s) => s
      case _ => sample(1000000)
    }

    implicit val Rand: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(actualSeed)))
    val concentrationParam = hyperPrior.draw()

    val betaPrior = new Beta(1,concentrationParam)(Rand)
    val betaDraw: List[Double] = (0 until size).map(_ => betaPrior.draw()).toList

    val pi: List[Double] = if(size == 1){
      betaDraw
    } else {
      betaDraw.head +: (1 until betaDraw.length).map(j => {
        betaDraw(j) * betaDraw.dropRight(betaDraw.length - j).map(1-_).product
      }).toList
    }
    pi
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

  def map_local_global_partition(cluster_partition: List[(Int, Int)],
                                 local_k_cluster: List[Int]):
  List[(Int, Int, Int)] = {
    (cluster_partition zip local_k_cluster).map(e => (e._1._1, e._2, e._1._2))
  }

  /**
   * Ds: global_line_partition computes the global line (row or column) membership using worker membership
   * Input:worker_result List(worker_id,local_membership,line_index) of size number_of_lines
   * master_result List(worker_id,global_membership) of size number_of_clusters
   * Output: List(List(index,global_membership,local_membership)) for size (number_of_clusters * number_of_lines)
   * */
  def global_line_partition(
                             worker_result: List[(Int, Int, Int)],
                             master_result: List[(Int, Int)],
                             local_k_cluster: List[Int]): List[List[(Int, Int, Int)]] = {
    val local_global_membership = (master_result zip local_k_cluster).map(e => (e._1._1, e._2, e._1._2))
    val result = worker_result.map(line => {
      val global_k = local_global_membership.filter(x => {
        x._1 == line._1 && x._2 == line._2
      }).head._3
      (line._1, line._3, global_k, line._2)
    })
    val tmp = SortedMap(result.groupBy(_._1).toSeq: _*).values.toList
    tmp.map(_.map(e => {
      (e._2, e._3, e._4)
    }))
  }

  def BlockSufficientStatistics_indexes(map_localPart_globalPart: List[(Int, Int, Int)],
                                        countRow: Int,
                                        countCol: Int,
                                        partitionOtherDimension: List[Int]):
                                        ListBuffer[ListBuffer[List[(Int,Int,Int)]]]={
    var result: ListBuffer[ListBuffer[List[(Int,Int,Int)]]] = ListBuffer()
    val index_columns_in_colCluster = partitionOtherDimension.zipWithIndex.groupBy(_._1)
    for (i <- 0 to countCol) {
      val row_NIWs_indexes: ListBuffer[List[(Int,Int,Int)]] = ListBuffer()
      for (j <- 0 to countRow) {
        val map_SufficientStatistics_j = map_localPart_globalPart.filter(_._3 == j).map(e => {
          (e._1, e._2)
        })
        val list_col_clusters = index_columns_in_colCluster.filter(_._1 == i).head._2.map(_._2)
        val SufficientStatistics_j = map_SufficientStatistics_j.indices.map(index_row => {
          val tmp = map_SufficientStatistics_j(index_row)
          val t1 = list_col_clusters.par.map(index_col => {
            (tmp._1,index_col,tmp._2)
          }).toList
          t1
        }).toList.flatten
        row_NIWs_indexes.append(SufficientStatistics_j)
      }
      result.append(row_NIWs_indexes)
    }
    result
  }
  /**
   * Ds: global_NIW_row computes global blocks' NIWs after row clustering using workers blocks' sufficient statistics
   * Input: map_localPart_globalPart List[(worker_id,local_cluster_id,global_cluster_id)]
   * countRow : Int = number of row clusters,
   * countCol :Int = number of columns clusters
   * BlockSufficientStatistics : List(List(List((mean,covariance,weight)))) the first list represent the workers,
   * the second one represent column wise matrix sufficient statistics
   * Output : Global column wise  NIWs matrix
   * */
   def global_NIW_row(
                              map_localPart_globalPart: List[(Int, Int, Int)],
                              countRow: Int,
                              countCol: Int,
                              BlockSufficientStatistics: List[
                                List[List[(DenseVector[Double], DenseMatrix[Double], Int)]]],
                              partitionOtherDimension: List[Int],
                              prior: NormalInverseWishart):
  ListBuffer[ListBuffer[NormalInverseWishart]] = {
    var result: ListBuffer[ListBuffer[NormalInverseWishart]] = ListBuffer()
    val index_columns_in_colCluster = partitionOtherDimension.zipWithIndex.groupBy(_._1)
    for (i <- 0 to countCol) {
      val row_NIWs: ListBuffer[NormalInverseWishart] = ListBuffer()
      for (j <- 0 to countRow) {
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
}
