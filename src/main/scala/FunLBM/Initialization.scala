package FunLBM

import Common.ProbabilisticTools._
import Common.Tools._
import breeze.linalg.{DenseMatrix, DenseVector, min}
import breeze.stats.distributions.MultivariateGaussian

import scala.util.Random

object Initialization  {

  def initialize(data: DenseMatrix[DenseVector[Double]],
                 K:Int, L:Int,
                 n:Int, p :Int,
                 initMethod: String = "randomPartition",
                 fullCovariance: Boolean,
                 verbose:Boolean = false): (FunLBM.LatentBlockModel, List[Int]) = {

    val nSampleForLBMInit = min(n, 50)
    initMethod match {
      case "sample" =>
        Initialization.initFromComponentSample(data, K, L, p, nSampleForLBMInit,verbose)
      case "randomPartition" =>
        initLBMFromRandomPartition(data, K, L, n,p, fullCovariance, verbose)
      case _ => {
        println(s"Warning: No initial method has been provided and initMethod $initMethod provided " +
          "does not match possible initialization method name (\"random\",\"randomPartition\")" +
          "Continuing with random partition initialization..")
        initLBMFromRandomPartition(data, K, L, n,p,verbose)
      }
    }
  }

  def initFromComponentSample(data: DenseMatrix[DenseVector[Double]],
                              K:Int,
                              L:Int,
                              p:Int,
                              nSamples:Int = 10,
                              verbose: Boolean = false): (LatentBlockModel, List[Int]) = {

    if(verbose) println("sample Initialization")

    val sampleBlock: List[(Int,Int)] = Random.shuffle(((0 until data.rows) cross (0 until data.cols)).toList).take(K*L*nSamples)
    val sampleData:List[DenseVector[Double]] = sampleBlock.map(idx => {data(idx._1,idx._2)})

    val newModels = (0 until L).map(l => {
      (0 until K).map(k => {
        val idxBlock = k*L+l
        val sampleBlock: List[DenseVector[Double]] = sampleData.slice(idxBlock*nSamples, (idxBlock+1)*nSamples)
        val mode: DenseVector[Double] = meanListDV(sampleBlock)
        MultivariateGaussian(mode, Common.ProbabilisticTools.covariance(sampleBlock,mode))
      }).toList
    }).toList

    val rowProportions: List[Double] = List.fill(K)(1.0 / K)
    val colProportions: List[Double] = List.fill(L)(1.0 / L)
    val colPartition = (0 until p).map(j => sample(colProportions)).toList

    (LatentBlockModel(rowProportions, colProportions, newModels), colPartition)
  }


  def initLBMFromRandomPartition(data: DenseMatrix[DenseVector[Double]],
                                 K:Int,
                                 L:Int,
                                 n:Int,
                                 p:Int,
                                 fullCovariance: Boolean,
                                 verbose: Boolean = false): (LatentBlockModel, List[Int]) = {

    if(verbose){println("Random Partition Initialization")}

    var rowPartition: List[Int] = Random.shuffle((0 until n).map(i => i%K)).toList
    var colPartition: List[Int] = Random.shuffle((0 until p).map(j => j%L)).toList
    var initModel =new LatentBlockModel(K,L).SEMGibbsMaximizationStep(data, rowPartition, colPartition,n,p, fullCovariance)
    (initModel,colPartition)
  }

}

