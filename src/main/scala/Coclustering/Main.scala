package Coclustering

import Coclustering.Common.NormalInverseWishart
import Coclustering.Common.Tools._
import Coclustering.DisNPLBM.DisNPLBM
import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.Gamma
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty
import java.io.{File, PrintWriter}
import scala.io.Source
class Line(workerId:Int, data: List[(Int,List[DenseVector[Double]])]) extends Serializable {
           val ID:Int = workerId
           val Data: List[(Int,List[DenseVector[Double]])] = data
      }

object Main {
  def main(args: Array[String]) {

    val sparkMaster = args(0)
    val datasetName = args(1)
    val datasetPath = args(2)
    val numberPartitions = args(3).toInt
    val taskCores = args(4)
    val nIter = args(5).toInt
    val dim = args(6).toInt
    val alpha = args(7).toDouble
    val beta = args(8).toDouble

    // Set concentration parameters
    val actualAlpha: Double = alpha
    val actualBeta: Double = beta
    implicit val formats: DefaultFormats.type = DefaultFormats

    // Load data
    val dataList =scala.io.Source.fromFile(s"$datasetPath/data/${datasetName}_data.csv").getLines().drop(1).toList.par.
      map(_.split(",").map(e => {
        if (dim == 1) {
          Array(e.toDouble)
        } else {
          e.split(":").map(k => k.toDouble)}}).map(e => DenseVector(e)).toList).toList.transpose


    // Load true labels
    val datasetInfo: Map[String, List[Any]] = JsonMethods.parse(Source.fromFile(s"$datasetPath/data/" + s"${datasetName}_info.json").reader()).extract[Map[String, List[Any]]]
    //val trueRowPartition: List[Int] = trueLabels("rowPartition")
    //val trueColPartition: List[Int] = trueLabels("colPartition")
    val trueBlockPartition: List[Int] = datasetInfo("blockPartition").map(_.toString.toInt)
    val empricalMean: DenseVector[Double] = DenseVector(datasetInfo("empiricalMean").map(_.toString.toDouble):_*)
    val empiricalPrecision: DenseMatrix[Double] = DenseMatrix(datasetInfo("empiricalCovariance").map(_.toString.toDouble): _*).reshape(empricalMean.length, empricalMean.length)

    // Set prior
    val userNiwPrior = new NormalInverseWishart(mu = empricalMean, kappa = 1, psi = empiricalPrecision, nu = empricalMean.length + 1)

    // Set spark configuration
    val conf = new SparkConf().setMaster(sparkMaster)
                            .setAppName("DisNPLBM")
                            .set("spark.scheduler.mode", "FAIR")
                            .set("spark.task.cpus", taskCores)
    val sc = new SparkContext(conf)
    val dataByRowRDD = sc.parallelize(dataList.transpose.zipWithIndex.map(e => (e._2, e._1)), numberPartitions)
    val workerRowRDD = dataByRowRDD.mapPartitionsWithIndex((index, data) => {Iterator(new Line(index, data.toList))})

    // Run DisNPLBM
    val t0 = System.nanoTime()
    val (rowMembership, colMembership) = new DisNPLBM(dataRDD = workerRowRDD,
                                                      alpha = actualAlpha,
                                                      beta = actualBeta,
                                                      initByUserPrior = Some(userNiwPrior)).run(maxIter = nIter)
    val t1 = System.nanoTime()

    // Compute runtime and scores
    val runtime =  (t1 - t0) / 1e9D
    val blockPartition = getBlockPartition(rowMembership, colMembership)
    val (ari, nmi, nClusters) = getScores(blockPartition, trueBlockPartition)
    val results = Map("ari" -> ari,
                      "nmi" -> nmi,
                      "nClusters" -> nClusters,
                      "runtime" -> runtime,
                      "rowMembership" -> rowMembership,
                      "colMembership" -> colMembership)
    val finalResults = writePretty(results)

    println(s"Runtime: $runtime")
    println(s"ARI: $ari")
    println(s"NMI: $nmi")
    println(s"Number of blocks: $nClusters")

    // Save results
    val f = new File(s"$datasetPath/results/${datasetName}_results.json")
    val w = new PrintWriter(f)
    w.write(finalResults)
    w.close()
  }
}