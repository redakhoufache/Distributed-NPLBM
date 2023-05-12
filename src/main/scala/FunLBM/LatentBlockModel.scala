package FunLBM

import Common.Tools._
import Common.ProbabilisticTools._
import breeze.linalg.{*, DenseMatrix, DenseVector, argmax, min, sum}
import breeze.numerics.{exp, log, round}
import breeze.stats.distributions.MultivariateGaussian

case class LatentBlockModel(proportionsRows: List[Double],
                            proportionsCols: List[Double],
                            gaussians: List[List[MultivariateGaussian]]
                           ) {
  val precision = 1e-8

  def K: Int = gaussians.head.length
  def L: Int = gaussians.length


  def this() {
    this(
      List(0D),
      List(0D)
      ,List(List(MultivariateGaussian(
        DenseVector(0D),
        DenseMatrix(1D)))))
  }

  // Auxiliary constructor
  def this(K:Int,
           L:Int) {
    this(List.fill(K)(1/K.toDouble),
      List.fill(L)(1/L.toDouble),
      (0 until L).map(l => {
        (0 until K).map(k => {
          MultivariateGaussian(
            DenseVector.zeros[Double](1),
            DenseMatrix.ones[Double](1,1))
        }).toList
      }).toList)
  }

  def computeJointDistribRows(data: DenseMatrix[DenseVector[Double]],
                              jointLogDistribCol: List[List[Double]],
                              n: Int, p: Int): List[List[Double]] = {
    val logPi: List[Double] = this.proportionsRows.map(log(_))

    (0 until n).par.map(i => {
      val row = (0 until K).map(k => {
        val listWLogf: DenseVector[Double] = DenseVector(((0 until p) cross (0 until L)).par.map(jl => {
          jointLogDistribCol(jl._1)(jl._2) * this.gaussians(jl._2)(k).logPdf(data(i,jl._1))
        }).toArray)
        logPi(k) + sum(listWLogf)
      }).toList
      val LSE = logSumExp(row)
      row. map(e => exp(e - LSE))
    }).toList

  }

  def computeJointDistribCols(data: DenseMatrix[DenseVector[Double]],
                              jointLogDistribRow: List[List[Double]],
                              n: Int, p: Int): List[List[Double]] = {
    val logRho: List[Double] = this.proportionsCols.map(log(_))
    (0 until p).map(j => {
      val row = (0 until L).map(l => {
        val listWLogf: DenseVector[Double] = DenseVector(((0 until n) cross (0 until K)).par.map(ik => {
          jointLogDistribRow(ik._1)(ik._2)*this.gaussians(l)(ik._2).logPdf(data(ik._1,j))
        }).toArray)
        logRho(l) + sum(listWLogf)
      }).toList
      val LSE = logSumExp(row)
      row. map(e => exp(e - LSE))
    }).toList

  }

  def computeJointLogDistribRowsFromSample(data: DenseMatrix[DenseVector[Double]], colPartition: List[Int]): List[List[Double]] = {

    // Vector of log p(z=k ; theta)
    val logPiRows: List[Double] = this.proportionsRows.map(log(_))
    val logPdfPerRowComponentMat: List[List[Double]] = (0 until data.rows).par.map(idxRow => {
      (0 until K).map(k => {
        val f_k = (0 until data.cols).map(idxCol => {
          this.gaussians(colPartition(idxCol))(k).logPdf(data(idxRow,idxCol))
        }).sum
        logPiRows(k) + f_k
      }).toList
    }).toList
    logPdfPerRowComponentMat
  }

  def computeJointLogDistribColsFromSample(data: DenseMatrix[DenseVector[Double]],
                                           RowPartition: List[Int]): List[List[Double]] = {

    // Vector of log p(z=k ; theta)
    val logPiCols: List[Double] = this.proportionsCols.map(log(_))

    val logPdfPerRowComponentMat: List[List[Double]] = (0 until data.cols).par.map(idxCol => {
      (0 until L).map(l => {
        val f_l = (0 until data.rows).map(idxRow => {
          this.gaussians(l)(RowPartition(idxRow)).logPdf(data(idxRow,idxCol))
        }).sum
        logPiCols(l) + f_l
      }).toList
    }).toList

    logPdfPerRowComponentMat
  }

  def drawRowPartition(data: DenseMatrix[DenseVector[Double]], colPartition: List[Int]) : List[Int]= {
    val jointLogDistribRows = computeJointLogDistribRowsFromSample(data, colPartition)
    val rowPartition:List[Int] = jointLogDistribRows.par.map(x => {
      val LSE = logSumExp(x)
      sample(x. map(e => exp(e - LSE)))
    }).toList
    rowPartition
  }

  def drawColPartition(data: DenseMatrix[DenseVector[Double]], rowPartition: List[Int]) : List[Int]= {
    val jointLogDistribCols = computeJointLogDistribColsFromSample(data, rowPartition)
    val colPartition:List[Int] = jointLogDistribCols.par.map(x => {
      val LSE = logSumExp(x)
      sample(x. map(e => exp(e - LSE)))
    }).toList
    colPartition
  }

  def expectationStep(data: DenseMatrix[DenseVector[Double]],
                      jointLogDistribCols: List[List[Double]],
                      n: Int, p:Int,
                      nIter: Int = 3,
                      verbose: Boolean = true): (List[List[Double]],List[List[Double]]) = {
    var newJointLogDistribRows: List[List[Double]] = computeJointDistribRows(data, jointLogDistribCols, n,p)
    var newJointLogDistribCols: List[List[Double]] = computeJointDistribCols(data, newJointLogDistribRows, n,p)
    if(nIter > 1) {
      for(_ <- 1 until nIter) {
        newJointLogDistribRows = computeJointDistribRows(data, newJointLogDistribCols,n,p)
        newJointLogDistribCols = computeJointDistribCols(data, newJointLogDistribRows,n,p)
      }
    }
    (newJointLogDistribRows,newJointLogDistribCols)
  }

  def maximizationStep(data: DenseMatrix[DenseVector[Double]],
                       jointLogDistribRows: List[List[Double]],
                       jointLogDistribCols: List[List[Double]],
                       fullCovariance: Boolean,
                       verbose: Boolean = true): LatentBlockModel = {

    val n = data.rows
    val p = data.cols

    val jointLogRowMat = DenseMatrix(jointLogDistribRows:_*)
    val jointLogColMat = DenseMatrix(jointLogDistribCols:_*)

    val newRowProportions: List[Double] = (sum(jointLogRowMat(::, *)).t / data.rows.toDouble).toArray.toList
    val newColProportions: List[Double] = (sum(jointLogColMat(::, *)).t / data.cols.toDouble).toArray.toList

    val weightMatrices = (0 until L).map(l => {(0 until K).map(k => {jointLogRowMat(::,k) * jointLogColMat(::,l).t})})
    val sumWeightMatrices = DenseMatrix(weightMatrices.map(_.map(sum(_))):_*)

    val weightedSums: DenseMatrix[DenseVector[Double]] = (0 until n).par.map(i => {
      (0 until p).map(j => {
        DenseMatrix.tabulate[DenseVector[Double]](K,L){
          (k,l) => weightMatrices(l)(k)(i,j)*data(i,j)
        }
      }).reduce(_+_)
    }).reduce(_+_)

    val means = (0 until L).map(l => {
      (0 until K).map(k => {
        weightedSums(k,l)/sumWeightMatrices(k,l)
      })
    })

    val unitCovFunction = unitCovFunc(fullCovariance)

    val sumProductCentered: DenseMatrix[DenseMatrix[Double]] = (0 until n).map(i => {
      (0 until p).map(j => {
        DenseMatrix.tabulate[DenseMatrix[Double]](K,L)(
          (k, l) => {
            val valueCentered = data(i,j) - means(l)(k)
            jointLogDistribRows(i)(k) * jointLogDistribCols(j)(l) * unitCovFunction(valueCentered)
          }
        )
      }).reduce(_+_)
    }).reduce(_+_)

    val covMatrices = DenseMatrix.tabulate[DenseMatrix[Double]](this.K, this.L) { (k, l) =>
    {sumProductCentered(k, l) / sumWeightMatrices(k, l)}}

    val newMultivariateGaussians: List[List[MultivariateGaussian]] = (0 until L).par.map(l =>
    {(0 until K).par.map(k => {
      val weightMatrix: DenseMatrix[Double] = weightMatrices(l)(k)
      val mean: DenseVector[Double] = means(l)(k)
      val covariance: DenseMatrix[Double] = weightedCovariance(data.t.toArray.toList,weightMatrix.t.toDenseVector,mean)
      MultivariateGaussian(mean,covariance)
    }).toList
    }).toList

    //    println("-- models")
    //    newMultivariateGaussians.flatten.foreach(g => println(g.meanListDV, g.covariance))
    LatentBlockModel(newRowProportions, newColProportions, newMultivariateGaussians)
  }

  def SEMGibbsExpectationStep(data: DenseMatrix[DenseVector[Double]],
                              colPartition: List[Int],
                              nIter: Int = 3,
                              verbose: Boolean = false): (List[Int],List[Int]) = {

    if(verbose){
      println(">> Begin Stochastic-Expectation step")
      println("- model:")
      println(this.gaussians)
      println("- Column clusters proportions: ")
      println(this.proportionsCols)
      println("- Row clusters proportions: ")
      println(this.proportionsRows)
    }

    var newRowPartition: List[Int] = drawRowPartition(data, colPartition)
    var newColPartition: List[Int] = drawColPartition(data, newRowPartition)

    if(verbose){
      println(newRowPartition cross newColPartition)
      println((newRowPartition cross newColPartition).groupBy(identity).mapValues(_.size))
    }

    if(nIter > 1) {
      for(iter <- 1 until nIter) {
        newRowPartition = drawRowPartition(data, newColPartition)
        newColPartition = drawColPartition(data, newRowPartition)
      }
    }

    if(verbose){
      println("End Stochastic-Expectation step")
    }

    (newRowPartition,newColPartition)
  }

  def SEMGibbsMaximizationStep(data: DenseMatrix[DenseVector[Double]],
                               rowPartition: List[Int],
                               colPartition: List[Int],
                               n:Int,
                               p:Int,
                               fullCovariance: Boolean,
                               verbose:Boolean = false): LatentBlockModel = {

    if(verbose){
      println(">> Begin Maximization")
      println("- rowPartition:")
      println(rowPartition)
      println("- colPartition:")
      println(colPartition)
    }

    val blockPartition = (rowPartition cross colPartition).toList
    val dataList: List[DenseVector[Double]] = data.t.toArray.toList
    val dataPartition = dataList zip blockPartition

    val dataAndSizeByBlock = (0 until this.L).map(l => {
      (0 until this.K).map(k => {
        val filteredData: List[DenseVector[Double]] = dataPartition.filter(_._2==(k,l)).map(_._1).toArray.toList
        val sizeBlock: Int = filteredData.length
        require(filteredData.nonEmpty, "Algorithm could not converge: empty block")
        (filteredData, sizeBlock)
      })
    })

    val unitCovFunction = unitCovFunc(fullCovariance)

    val newModels: List[List[MultivariateGaussian]] = (0 until L).par.map(l =>
    {(0 until K).par.map(k => {
      val dataBlock = dataAndSizeByBlock(l)(k)._1
      val sizeBlock = dataAndSizeByBlock(l)(k)._2
      val mode:DenseVector[Double] = dataBlock.reduce(_+_) / sizeBlock.toDouble
      val covMat: DenseMatrix[Double] = dataBlock.map(v => {
        val centeredRow: DenseVector[Double] = v - mode
        unitCovFunction(centeredRow)}).reduce(_+_) / sizeBlock.toDouble
      MultivariateGaussian(mode, covMat)
    }).toList
    }).toList

    val proportionsRows: List[Double] = rowPartition.groupBy(identity).mapValues(_.size.toDouble / n).toList.sortBy(_._1).map(_._2 + precision)
    val proportionCols: List[Double] = colPartition.groupBy(identity).mapValues(_.size.toDouble / p).toList.sortBy(_._1).map(_._2 + precision)

    if(verbose){println("End Maximization")}

    LatentBlockModel(proportionsRows, proportionCols, newModels)
  }

  def completeLogLikelihood(data: DenseMatrix[DenseVector[Double]],
                            rowPartition: List[Int],
                            colPartition: List[Int]): Double = {

    val logRho: List[Double] = this.proportionsCols.map(log(_))
    val logPi: List[Double]  = this.proportionsRows.map(log(_))

    (0 until data.cols).par.map(j => {
      (0 until data.rows).map(i => {
        val k = rowPartition(i)
        val l = colPartition(j)
        logPi(k)
        + logRho(l)
        + this.gaussians(l)(k).logPdf(data(i,j))
      }).sum
    }).sum

  }

  def completeLogLikelihoodAfterMAP(data: DenseMatrix[DenseVector[Double]],
                                    jointLogDistribRows: List[List[Double]],
                                    jointLogDistribCols: List[List[Double]]
                                   ): Double = {

    val rowPartition =  jointLogDistribRows.map(e => Common.Tools.argmax(e))
    val colPartition =  jointLogDistribCols.map(e => Common.Tools.argmax(e))
    completeLogLikelihood(data,rowPartition, colPartition)
  }

  def ICL(completelogLikelihood: Double,
          n: Double,
          p: Double,
          fullCovariance: Boolean): Double = {

    val dimVar = this.gaussians.head.head.mean.size
    val nParamPerComponent = if(fullCovariance){
      dimVar + dimVar * (dimVar + 1) / 2D
    } else {
      2D * dimVar
    }
    completelogLikelihood - log(n) * (this.K - 1) / 2D - log(p) * (L - 1) / 2D - log(n * p) * (K * L * nParamPerComponent) / 2D
  }
}

