package FunLBM

import Common.Tools._
import Common.ProbabilisticTools._
import breeze.linalg.{DenseMatrix, DenseVector, argmax, max}
import breeze.numerics.abs
import breeze.stats.distributions.MultivariateGaussian

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Random, Success, Try}

class LatentBlock private (private var K: Int,
                           private var L: Int,
                           private var maxIterations: Int,
                           private var maxBurninIterations: Int,
                           private var fullCovarianceHypothesis: Boolean = true,
                           private var seed: Long) extends Serializable {

  val precision = 1e-2

  /**
    * Constructs a default instance. The default parameters are {k: 2, convergenceTol: 0.01,
    * maxIterations: 100, seed: random}.
    */
  def this() = this(2,2, 7,7, seed = Random.nextLong())

  // number of samples per cluster to use when initializing Gaussians
  private val nSamples = 5

  // an initializing model can be provided rather than using the
  // default random starting point
  private var providedInitialModel: Option[LatentBlockModel] = None
  private var providedInitialColPartition: Option[List[Int]] = None

  /**
    * Set the initial GMM starting point, bypassing the random initialization.
    * You must call setK() prior to calling this method, and the condition
    * (model.k == this.k) must be met; failure will result in an IllegalArgumentException
    */
  def setInitialModel(model: LatentBlockModel): this.type = {
    require( model.K == K,
      s"Mismatched row cluster number (model.K ${model.K} != K $K)")
    require( model.L == L,
      s"Mismatched column cluster number (model.L ${model.L} != L $L)")
    providedInitialModel = Some(model)
    this
  }

  /**
    * Return the user supplied initial GMM, if supplied
    */
  def getInitialModel: Option[LatentBlockModel] = providedInitialModel

  /**
    * Set the number of Gaussians in the mixture model.  Default: 2
    */
  def setK(K: Int): this.type = {
    require(K > 0,
      s"Number of row clusters must be positive but got $K")
    this.K = K
    this
  }

  /**
    * Set the number of Gaussians in the mixture model.  Default: 2
    */
  def setL(L: Int): this.type = {
    require(L > 0,
      s"Number of column clusters must be positive but got $L")
    this.L = L
    this
  }
  
  /**
    * Return the number of row cluster number in the latent block model
    */
  def getK: Int = K

  /**
    * Return the number of column cluster number in the latent block model
    */
  def getL: Int = L

  /**
    * Set the maximum number of iterations allowed. Default: 100
    */
  def setMaxIterations(maxIterations: Int): this.type = {
    require(maxIterations >= 0,
      s"Maximum of iterations must be nonnegative but got $maxIterations")
    this.maxIterations = maxIterations
    this
  }

  /**
    * Set the maximum number of iterations allowed. Default: 100
    */
  def setMaxBurninIterations(maxBurninIterations: Int): this.type = {
    require(maxBurninIterations >= 0,
      s"Maximum of Burn-in iterations must be nonnegative but got $maxBurninIterations")
    this.maxBurninIterations = maxBurninIterations
    this
  }

  /**
    * Return the maximum number of iterations allowed
    */
  def getMaxIterations: Int = maxIterations

  /**
    * Return the maximum number of iterations allowed
    */
  def getMaxBurninIterations: Int = maxBurninIterations

  /**
    * Set the random seed
    */
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /**
    * Return the random seed
    */
  def getSeed: Long = seed

  def computeMeanModels(models: List[LatentBlockModel]): LatentBlockModel = {

    val meanProportionRows: List[Double] = (models.map(model =>
      DenseVector(model.proportionsRows.toArray)).reduce(_+_)/models.length.toDouble).toArray.toList

    val meanProportionCols: List[Double] = (models.map(model =>
      DenseVector(model.proportionsCols.toArray)).reduce(_+_)/models.length.toDouble).toArray.toList

    val meanMu: DenseMatrix[DenseVector[Double]] = DenseMatrix.tabulate(K,L){ (k, l)=>{
      models.map(m => m.gaussians(l)(k).mean).reduce(_+_)/models.length.toDouble
    }}

    val meanCovariance: DenseMatrix[DenseMatrix[Double]] = DenseMatrix.tabulate(K,L){ (k, l)=>{
      models.map(m => m.gaussians(l)(k).covariance).reduce(_+_)/models.length.toDouble
    }}

    val meanGaussians: List[List[MultivariateGaussian]] = (0 until L).map(l => {
      (0 until K).map(k => {
        MultivariateGaussian(meanMu(l,k), meanCovariance(k,l))
      }).toList
    }).toList

    LatentBlockModel(meanProportionRows, meanProportionCols, meanGaussians)
  }

  def run(data: DenseMatrix[DenseVector[Double]],
          EMMethod: String= "SEMGibbs",
          verbose: Boolean = false,
          nConcurrent:Int = 10,
          nTryMaxPerConcurrent:Int = 20,
          initMethod: String = "randomPartition"): Map[String,Product] = {

    require(List("SEMGibbs","VariationalEM").contains(EMMethod),
      "EM Method provided "+EMMethod+" is not available. Possible options: SEMGibbs, VariationalEM")

    val allRes = (0 until nConcurrent).map(nTry => {
      if(verbose){println("> n° launch "+(1+nTry).toString+"/"+nConcurrent.toString)}
      this.initAndRunTry(data,EMMethod, nTryMax = nTryMaxPerConcurrent, initMethod = initMethod, verbose=verbose)
    })
    val allLikelihoods: DenseVector[Double] = DenseVector(allRes.map(_("LogLikelihood").asInstanceOf[List[Double]].last).toArray)
    if(verbose){println("best LogLikelihood: " + max(allLikelihoods).toString)}
    allRes(argmax(allLikelihoods))
  }

  def initAndRunTry(data: DenseMatrix[DenseVector[Double]],
                    EMMethod:String = "SEMGibbs",
                    nTryMax: Int = 50,
                    initMethod: String = "randomPartition",
                    verbose: Boolean=false): Map[String,Product] = {

    val t0 = System.nanoTime()
    val logLikelihoodList: ListBuffer[Double] = new ListBuffer[Double]() += Double.NegativeInfinity
    val ICL: ListBuffer[Double] = new ListBuffer[Double]() += Double.NegativeInfinity
    val p:Int = data.cols
    val n:Int = data.rows
    val emptyResult = Map("Model" -> new FunLBM.LatentBlockModel(),
      "ColPartition" -> List.fill(p)(0),
      "RowPartition" -> List.fill(n)(0),
      "LogLikelihood" -> logLikelihoodList.toList,
      "ICL" -> ICL.toList)

    @tailrec def go(iter: Int): Map[String,Product]=
    {
      if(iter > nTryMax){
        emptyResult
      } else {
        Try(this.initAndLaunch(data, EMMethod, n,p, initMethod = initMethod, verbose=verbose)) match {
          case Success(v) =>
            if(verbose){println()
              printTime(t0, EMMethod+" LBM")}
            Success(v).get.asInstanceOf[Map[String, Product with Serializable]]
          case Failure(_) =>
            if(verbose){
              if(iter==1){
                print("Algorithm "+ EMMethod+" didn't converge to an appropriate solution, trying again..\n" +
                  "n° try: "+iter.toString+"")
              } else {print(", "+iter.toString)}}
            go(iter+1)
        }
      }
    }
    go(0)
  }

  def initAndLaunch(data: DenseMatrix[DenseVector[Double]],
                    EMMethod: String,
                    n: Int,
                    p: Int,
                    verbose:Boolean = false,
                    initMethod: String = "randomPartition"): Map[String,Serializable]= {

    val (initialModel, initialColPartition) = providedInitialModel match {
      case Some(model) =>
        require(initMethod.isEmpty,
          s"An initial model has been provided but initMethod argument has also been set to $initMethod. " +
            s"Please make a choice: do not set an initMethod or do not provide an initial model.")
        (model,
          providedInitialColPartition match {
            case Some(colPartition) => colPartition
            case None => (0 until p).map(_ => sample(model.proportionsCols)).toList
          })
      case None => Initialization.initialize(data,K,L,n,p,initMethod, this.fullCovarianceHypothesis)
    }

    EMMethod match {
      case "SEMGibbs" => SEMGibbs(data, initialModel, initialColPartition, n,p, verbose)
      case "VariationalEM" => variationalEM(data, initialModel, initialColPartition, n,p, verbose)

    }
  }

  def SEMGibbs(data: DenseMatrix[DenseVector[Double]],
               initialModel: LatentBlockModel,
               initialColPartition: List[Int],
               n:Int,
               p:Int,
               verbose:Boolean=false): Map[String, Serializable] = {
    var precColPartition: List[Int] = initialColPartition
    var precModel = initialModel
    var precRowPartition = initialModel.drawRowPartition(data, precColPartition)

    var completeLogLikelihoodList: ListBuffer[Double] = ListBuffer.empty ++= List(
      Double.NegativeInfinity,
      precModel.completeLogLikelihood(data, precRowPartition, precColPartition))

    var cachedModels = List[FunLBM.LatentBlockModel]():+precModel
    var hasConverged: Boolean = false

    var iter = 0
    do {
      iter += 1
      val (newRowPartition, newColPartition) = precModel.SEMGibbsExpectationStep(data, precColPartition,  verbose = verbose)
      precModel = precModel.SEMGibbsMaximizationStep(data, newRowPartition, newColPartition, n, p, this.fullCovarianceHypothesis, verbose)
      precRowPartition = newRowPartition
      precColPartition = newColPartition
      completeLogLikelihoodList += precModel.completeLogLikelihood(data, newRowPartition, newColPartition)
      cachedModels = cachedModels :+ precModel
      hasConverged = abs(completeLogLikelihoodList.last - completeLogLikelihoodList.dropRight(1).last)<precision &
          abs(completeLogLikelihoodList.last - completeLogLikelihoodList.dropRight(2).last)<precision

    } while (iter < (maxBurninIterations+maxIterations) & !hasConverged)

    val iclList = completeLogLikelihoodList.map(ll => precModel.ICL(ll, n, p, this.fullCovarianceHypothesis))
    val resModel = if (!hasConverged){
      computeMeanModels(cachedModels.drop(1+maxBurninIterations))
    } else cachedModels.last

    Map("Model" -> resModel.asInstanceOf[Serializable],
      "RowPartition" -> precRowPartition.asInstanceOf[Serializable],
      "ColPartition" -> precColPartition.asInstanceOf[Serializable],
      "LogLikelihood" -> completeLogLikelihoodList.toList.asInstanceOf[Serializable],
      "ICL" -> iclList.toList.asInstanceOf[Serializable])
  }

  def variationalEM(data: DenseMatrix[DenseVector[Double]],
                    initialModel: LatentBlockModel,
                    initialColProbBelonging: List[Int],
                    n: Int,
                    p: Int,
                    verbose:Boolean = false): Map[String, Serializable] = {

    var precModel = initialModel
    var precColProbBelonging = partitionToBelongingProbabilities(initialColProbBelonging)
    var precRowProbBelonging = precModel.computeJointDistribRows(data, precColProbBelonging,n,p)

    var completeLogLikelihoodList: ListBuffer[Double] = ListBuffer.empty ++= List(
      Double.NegativeInfinity,
      precModel.completeLogLikelihoodAfterMAP(data, precRowProbBelonging, precColProbBelonging))

    var hasConverged: Boolean = false
    var iter = 0
    do {
      iter += 1
      val (newJointLogDistribRows, newJointLogDistribCols) = precModel.expectationStep(data, precColProbBelonging, n,p,verbose = verbose)
      precModel = precModel.maximizationStep(data, newJointLogDistribRows, newJointLogDistribCols,  this.fullCovarianceHypothesis, verbose)
      precRowProbBelonging = newJointLogDistribRows
      precColProbBelonging = newJointLogDistribCols

      if(verbose){
        println("iter: "+ iter.toString +", logLikelihood: "+ completeLogLikelihoodList.last.toString)
      }

      completeLogLikelihoodList += precModel.completeLogLikelihoodAfterMAP(
        data,
        precRowProbBelonging,
        precColProbBelonging)
      hasConverged = abs(completeLogLikelihoodList.last - completeLogLikelihoodList.dropRight(1).last)<precision &
        abs(completeLogLikelihoodList.last - completeLogLikelihoodList.dropRight(2).last)<precision
    } while (iter < (maxBurninIterations+maxIterations) & !hasConverged)


    val iclList = completeLogLikelihoodList.map(ll => precModel.ICL(ll, n, p,  this.fullCovarianceHypothesis))

    Map("Model" -> precModel,
      "RowProbBelonging" -> precRowProbBelonging.asInstanceOf[Serializable],
      "ColProbBelonging" -> precColProbBelonging.asInstanceOf[Serializable],
      "LogLikelihood" -> completeLogLikelihoodList.toList.asInstanceOf[Serializable],
      "ICL" -> iclList.toList.asInstanceOf[Serializable])

  }

}
