package FunNPLBM

import Common.NormalInverseWishart
import Common.ProbabilisticTools._
import Common.Tools._
import breeze.linalg.DenseVector
import breeze.numerics.log
import breeze.stats.distributions.Gamma

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

class CollapsedGibbsSampler(val Data: List[List[DenseVector[Double]]],
                            var alpha: Option[Double] = None,
                            var beta: Option[Double] = None,
                            var alphaPrior: Option[Gamma] = None,
                            var betaPrior: Option[Gamma] = None,
                            var initByUserPrior: Option[NormalInverseWishart] = None,
                            var initByUserRowPartition: Option[List[Int]] = None,
                            var initByUserColPartition: Option[List[Int]] = None) extends Serializable {

  val DataByRow = Data.transpose
  val n: Int = Data.head.length
  val p: Int = Data.length

  var prior: NormalInverseWishart = initByUserPrior match {
    case Some(pr) => pr
    case None => new NormalInverseWishart(Data)
  }

  val d: Int = prior.d
  require(prior.d == Data.head.head.length, "Prior and data dimensions differ")

  var rowPartition: List[Int] = initByUserRowPartition match {
    case Some(m) =>
      require(m.length == Data.head.length)
      m
    case None => List.fill(n)(0)
  }

  var colPartition: List[Int] = initByUserColPartition match {
    case Some(m) =>
      require(m.length == Data.length)
      m
    case None => List.fill(p)(0)
  }

  var countRowCluster: ListBuffer[Int] = partitionToOrderedCount(rowPartition).to[ListBuffer]
  var countColCluster: ListBuffer[Int] = partitionToOrderedCount(colPartition).to[ListBuffer]

  var NIWParamsByCol: ListBuffer[ListBuffer[NormalInverseWishart]] = (Data zip colPartition).groupBy(_._2).values.map(e => {
    val dataPerColCluster = e.map(_._1).transpose
    val l = e.head._2
    (l, (dataPerColCluster zip rowPartition).groupBy(_._2).values.map(f => {
      val dataPerBlock = f.map(_._1).reduce(_++_)
      val k = f.head._2
      (k, prior.update(dataPerBlock))
    }).toList.sortBy(_._1).map(_._2).to[ListBuffer])
  }).toList.sortBy(_._1).map(_._2).to[ListBuffer]

  var updateAlphaFlag: Boolean = checkAlphaPrior(alpha, alphaPrior)
  var updateBetaFlag: Boolean = checkAlphaPrior(beta, betaPrior)

  var actualAlphaPrior: Gamma = alphaPrior match {
    case Some(g) => g
    case None => new Gamma(1D,1D)
  }
  var actualBetaPrior: Gamma = betaPrior match {
    case Some(g) => g
    case None => new Gamma(1D,1D)
  }

  var actualAlpha: Double = alpha match {
    case Some(a) =>
      require(a > 0, s"AlphaRow parameter is optional and should be > 0 if provided, but got $a")
      a
    case None => actualAlphaPrior.mean
  }

  var actualBeta: Double = beta match {
    case Some(a) =>
      require(a > 0, s"AlphaCol parameter is optional and should be > 0 if provided, but got $a")
      a
    case None => actualBetaPrior.mean
  }

  def getRowPartition(): List[Int] = rowPartition
  def getColPartition(): List[Int] = colPartition

  def getNIWParams(): Seq[ListBuffer[NormalInverseWishart]] = NIWParamsByCol

  def checkAlphaPrior(alpha: Option[Double], alphaPrior: Option[Gamma]): Boolean = {
    require(!(alpha.isEmpty & alphaPrior.isEmpty),"Either alphaRow or alphaRowPrior must be provided: please provide one of the two parameters.")
    require(!(alpha.isDefined & alphaPrior.isDefined), "Providing both alphaRow or alphaRowPrior is not supported: remove one of the two parameters.")
    alphaPrior.isDefined
  }

  def priorPredictive(line: List[DenseVector[Double]],
                      partitionOtherDim: List[Int]): Double = {

    (line zip partitionOtherDim).groupBy(_._2).values.par.map(e => {
      val currentData = e.map(_._1)
      prior.jointPriorPredictive(currentData)
    }).toList.sum
  }

  def computeClusterMembershipProbabilities(x: List[DenseVector[Double]],
                                            partitionOtherDimension: List[Int],
                                            countCluster: ListBuffer[Int],
                                            NIWParams: ListBuffer[ListBuffer[NormalInverseWishart]],
                                            verbose: Boolean=false): List[Double] = {

    val xByRow = (x zip partitionOtherDimension).groupBy(_._2).map(v => (v._1, v._2.map(_._1)))
    NIWParams.indices.par.map(l => {
      (l, NIWParams.head.indices.par.map(k => {
        NIWParams(l)(k).jointPriorPredictive(xByRow(k))
      }).sum + log(countCluster(l)))
    }).toList.sortBy(_._1).map(_._2)
  }

  def drawMembership(x: List[DenseVector[Double]],
                     partitionOtherDimension: List[Int],
                     countCluster: ListBuffer[Int],
                     NIWParams: ListBuffer[ListBuffer[NormalInverseWishart]],
                     alpha: Double,
                     verbose : Boolean = false): Int = {

    val probPartition = computeClusterMembershipProbabilities(x, partitionOtherDimension, countCluster, NIWParams, verbose)
    val posteriorPredictiveXi = priorPredictive(x, partitionOtherDimension)
    val probs = probPartition :+ (posteriorPredictiveXi + log(alpha))
    val normalizedProbs = normalizeLogProbability(probs)
    sample(normalizedProbs)
  }

  private def removeElementFromRowCluster(row: List[DenseVector[Double]], currentPartition: Int): Unit = {
    if (countRowCluster(currentPartition) == 1) {
      countRowCluster.remove(currentPartition)
      NIWParamsByCol.map(_.remove(currentPartition))
      rowPartition = rowPartition.map(c => { if( c > currentPartition ){ c - 1 } else c })
    } else {
      countRowCluster.update(currentPartition, countRowCluster.apply(currentPartition) - 1)
      (row zip colPartition).groupBy(_._2).values.foreach(e => {
        val l = e.head._2
        val dataInCol = e.map(_._1)
        NIWParamsByCol(l).update(currentPartition, NIWParamsByCol(l)(currentPartition).removeObservations(dataInCol))
      })
    }
  }

  private def removeElementFromColCluster(column: List[DenseVector[Double]], currentPartition: Int): Unit = {
    if (countColCluster(currentPartition) == 1) {
      countColCluster.remove(currentPartition)
      NIWParamsByCol.remove(currentPartition)
      colPartition = colPartition.map(c => { if( c > currentPartition ){ c - 1 } else c })
    } else {
      countColCluster.update(currentPartition, countColCluster.apply(currentPartition) - 1)
      (column zip rowPartition).groupBy(_._2).values.foreach(e => {
        val k = e.head._2
        val dataInCol = e.map(_._1)
        NIWParamsByCol(currentPartition).update(k, NIWParamsByCol(currentPartition)(k).removeObservations(dataInCol))
      })
    }
  }

  private def addElementToRowCluster(row: List[DenseVector[Double]],
                                     newPartition: Int): Unit = {

    if (newPartition == countRowCluster.length) {
      countRowCluster = countRowCluster ++ ListBuffer(1)
      (row zip colPartition).groupBy(_._2).values.foreach(e => {
        val l = e.head._2
        val dataInCol = e.map(_._1)
        val newNIWparam = this.prior.update(dataInCol)
        NIWParamsByCol(l) = NIWParamsByCol(l) ++ ListBuffer(newNIWparam)
      })
    } else {
      countRowCluster.update(newPartition, countRowCluster.apply(newPartition) + 1)
      (row zip colPartition).groupBy(_._2).values.foreach(e => {
        val l = e.head._2
        val dataInCol = e.map(_._1)
        NIWParamsByCol(l).update(newPartition,
          NIWParamsByCol(l)(newPartition).update(dataInCol))
      })
    }
  }

  private def addElementToColCluster(column: List[DenseVector[Double]],
                                     newPartition: Int): Unit = {

    if (newPartition == countColCluster.length) {
      countColCluster = countColCluster ++ ListBuffer(1)
      val newCluster = (column zip rowPartition).groupBy(_._2).values.map(e => {
        val k = e.head._2
        val dataInRow = e.map(_._1)
        (k, this.prior.update(dataInRow))
      }).toList.sortBy(_._1).map(_._2).to[ListBuffer]
      NIWParamsByCol = NIWParamsByCol ++ ListBuffer(newCluster)
    } else {
      countColCluster.update(newPartition, countColCluster.apply(newPartition) + 1)
      (column zip rowPartition).groupBy(_._2).values.foreach(e => {
        val k = e.head._2
        val dataInCol = e.map(_._1)
        NIWParamsByCol(newPartition).update(k,
          NIWParamsByCol(newPartition)(k).update(dataInCol))
      })
    }
  }

  def updateRowPartition(verbose: Boolean = false) = {
    for (i <- DataByRow.indices) {
      val currentData = DataByRow(i)
      val currentPartition = rowPartition(i)
      removeElementFromRowCluster(currentData, currentPartition)
      val newPartition = drawMembership(currentData, colPartition, countRowCluster, NIWParamsByCol.transpose, actualAlpha)
      rowPartition = rowPartition.updated(i, newPartition)
      addElementToRowCluster(currentData, newPartition)
    }
  }

  def updateColPartition(verbose: Boolean = false) = {
    for (i <- Data.indices) {
      val currentData = Data(i)
      val currentPartition = colPartition(i)
      removeElementFromColCluster(currentData, currentPartition)
      val newMembership = drawMembership(currentData, rowPartition, countColCluster, NIWParamsByCol, actualBeta)
      colPartition = colPartition.updated(i, newMembership)
      addElementToColCluster(currentData, newMembership)
    }
  }

  def run(nIter: Int = 10,
          nIterBurnin: Int = 10,
          verbose: Boolean = false) = {

    @tailrec def go(rowPartitionEveryIteration: List[List[Int]],
                    colPartitionEveryIteration: List[List[Int]],
                    iter: Int):
    (List[List[Int]], List[List[Int]])= {

      if(verbose){
        println("\n>>>>>> Iteration: " + iter.toString)
        Common.Tools.prettyPrintLBM(countRowCluster.toList, countColCluster.toList)
      }

      if (iter > (nIter + nIterBurnin)) {

        (rowPartitionEveryIteration, colPartitionEveryIteration)

      } else {

        var t0 = System.nanoTime()

        updateRowPartition()

        if(verbose){
          t0 = printTime(t0, "draw row Partition")
        }

        updateColPartition()

        if(verbose){
          t0 = printTime(t0, "draw col Partition")
        }

        if(updateAlphaFlag){actualAlpha = updateAlpha(actualAlpha, actualAlphaPrior, countRowCluster.length, n)}
        if(updateBetaFlag){actualBeta = updateAlpha(actualBeta, actualBetaPrior, countColCluster.length, p)}

        go(rowPartitionEveryIteration :+ rowPartition,
          colPartitionEveryIteration :+ colPartition,
          iter + 1)
      }
    }

    val (rowPartitionEveryIterations, colPartitionEveryIterations) = go(List(rowPartition), List(colPartition), 1)

    // Final results
    //    val finalRowPartitions = new PartitionDistribution(rowPartitionEveryIterations.toArray.map(_.toArray).drop(nIterBurnin)) .run()
    //    val finalColPartitions = new PartitionDistribution(colPartitionEveryIterations.toArray.map(_.toArray).drop(nIterBurnin)) .run()
    //    val finalComponents = clustersParametersEstimation(Data, prior, finalRowPartitions, finalColPartitions)
    //    (finalRowPartitions, finalColPartitions, finalComponents)

    // Every iterations results
    val componentsEveryIterations = rowPartitionEveryIterations.indices.map(i => {
      //      println(rowPartitionEveryIterations(i).distinct,  colPartitionEveryIterations(i).distinct)
      val components = clustersParametersEstimation(Data, prior, rowPartitionEveryIterations(i), colPartitionEveryIterations(i))
      //      println(components.length, components.head.length)
      components
    }).toList
    (rowPartitionEveryIterations, colPartitionEveryIterations,  componentsEveryIterations)

  }


  def runWithFixedPartitions(nIter: Int = 10,
                             updateCol: Boolean = false,
                             updateRow: Boolean = true,
                             verbose: Boolean = false)  = {

    @tailrec def go(rowPartitionEveryIteration: List[List[Int]],
                    colPartitionEveryIteration: List[List[Int]],
                    iter: Int):
    (List[List[Int]], List[List[Int]])= {

      if(verbose){
        println("\n>>>>>> Iteration: " + iter.toString)
        Common.Tools.prettyPrintLBM(countRowCluster.toList, countColCluster.toList)
      }

      if (iter > nIter) {

        (rowPartitionEveryIteration, colPartitionEveryIteration)

      } else {

        var t0 = System.nanoTime()

        if(updateRow){

          updateRowPartition()

          if(verbose){
            t0 = printTime(t0, "draw row Partition")
          }

          if(updateAlphaFlag){actualAlpha = updateAlpha(actualAlpha, actualAlphaPrior, countRowCluster.length, n)}

        }

        if(updateCol){

          updateColPartition()

          if(verbose){
            t0 = printTime(t0, "draw col Partition")
          }

          if(updateBetaFlag){actualBeta = updateAlpha(actualBeta, actualBetaPrior, countColCluster.length, p)}

        }

        go(rowPartitionEveryIteration :+ rowPartition,
          colPartitionEveryIteration :+ colPartition,
          iter + 1)
      }
    }

    val (rowPartitionEveryIterations, colPartitionEveryIterations) = go(List(rowPartition), List(colPartition), 1)

    // Final results
    //    val finalRowPartitions = new PartitionDistribution(rowPartitionEveryIterations.toArray.map(_.toArray).drop(nIterBurnin)) .run()
    //    val finalColPartitions = new PartitionDistribution(colPartitionEveryIterations.toArray.map(_.toArray).drop(nIterBurnin)) .run()
    //    val finalComponents = clustersParametersEstimation(Data, prior, finalRowPartitions, finalColPartitions)
    //    (finalRowPartitions, finalColPartitions, finalComponents)

    // Every iterations results
    val componentsEveryIterations = rowPartitionEveryIterations.indices.map(i => {
      clustersParametersEstimation(Data, prior, rowPartitionEveryIterations(i), colPartitionEveryIterations(i))
    }).toList
    (rowPartitionEveryIterations, colPartitionEveryIterations,  componentsEveryIterations)

  }
}
