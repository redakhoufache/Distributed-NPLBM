package Common

import Common.ProbabilisticTools.unitCovFunc
import Common.Tools.prettyFormatLBM
import breeze.linalg.eigSym.EigSym
import breeze.linalg.{*, DenseMatrix, DenseVector, eigSym, max, min, sum}
import breeze.numerics.{abs, exp, log, sqrt}
import breeze.stats.distributions.RandBasis
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.{Matrices, Matrix}
import org.apache.spark.rdd.RDD
import smile.validation.{NormalizedMutualInformation, adjustedRandIndex, randIndex}

import scala.collection.immutable
import scala.util.{Success, Try}

object Tools extends java.io.Serializable {

  def relabel[T: Ordering](L: List[T]): List[Int] = {
    val uniqueLabels = L.distinct.sorted
    val dict = uniqueLabels.zipWithIndex.toMap
    L.map(dict)
  }

  def partitionToOrderedCount(partition: List[Int]): List[Int] = {
    partition.groupBy(identity).mapValues(_.size).toList.sortBy(_._1).map(_._2)
  }

  def prettyPrintLBM(countRowCluster: List[Int], countColCluster: List[Int]): Unit = {
    println(prettyFormatLBM(countRowCluster, countColCluster))
  }

  def prettyFormatLBM(countRowCluster: List[Int], countColCluster: List[Int]): DenseMatrix[String] = {
    val mat: DenseMatrix[String] = (DenseVector(countRowCluster.toArray) * DenseVector(countColCluster.toArray).t).map(i => i.toString)

    val rowName: DenseMatrix[String] = DenseMatrix.vertcat(
      DenseMatrix(countRowCluster.map(_.toString)),
      DenseMatrix(Array.fill(countRowCluster.length)("|"))).t

    val colName: DenseMatrix[String] = DenseMatrix.horzcat(
      DenseMatrix.vertcat(
        DenseMatrix(Array(" "," ")),
        DenseMatrix(Array(" "," "))
      ),
      DenseMatrix.vertcat(
        DenseMatrix(countColCluster.map(_.toString)),
        DenseMatrix(Array.fill(countColCluster.length)("—"))
      )
    )

    DenseMatrix.vertcat(
      colName,
      DenseMatrix.horzcat(rowName, mat)
    )
  }


  def prettyPrint(sizePerBlock: Map[(Int,Int), Int]): Unit = {
    val keys = sizePerBlock.keys
    val L = keys.map(_._1).max + 1
    val K = keys.map(_._2).max + 1
    val mat = DenseMatrix.tabulate[String](L,K){
      case (i, j) => if(sizePerBlock.contains((i,j))){
        sizePerBlock(i,j).toString } else {"-"}

    }
    println(mat.t)
  }

  def getPartitionFromSize(size: List[Int]): List[Int] = {
    size.indices.map(idx => List.fill(size(idx))(idx)).reduce(_ ++ _)
  }

  def argmax(l: List[Double]): Int ={
    l.view.zipWithIndex.maxBy(_._1)._2
  }

  def printTime(t0:Long, stepName: String, verbose:Boolean = true): Long={
    if(verbose){
      println(stepName.concat(" step duration: ").concat(((System.nanoTime - t0)/1e9D ).toString))
    }
    System.nanoTime
  }

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) : Traversable[(X,Y)] = for { x <- xs; y <- ys } yield (x, y)
  }

  def denseMatrixToMatrix(A: DenseMatrix[Double]): Matrix = {
    Matrices.dense(A.rows, A.cols, A.toArray)
  }


  def matrixToDenseMatrix(A: Matrix): DenseMatrix[Double] = {
    val p = A.numCols
    val n = A.numRows
    DenseMatrix(A.toArray).reshape(n,p)
  }


  def roundMat(m: DenseMatrix[Double], digits:Int=0): DenseMatrix[Double] = {
    m.map(round(_,digits))
  }

  def allEqual[T](x: List[T], y:List[T]): Boolean = {
    require(x.length == y.length)
    val listBool = x.indices.map(i => {x(i)==y(i)})
    listBool.forall(identity)
  }

  def getCondBlockPartition(rowPartition: List[List[Int]], colPartition: List[Int]): List[(Int, Int)] = {
    val blockPartitionMat = colPartition.par.map(l => {
      DenseMatrix.tabulate[(Int, Int)](rowPartition.head.length, 1) {
        (i, _) => (rowPartition(l)(i), l)
      }}).reduce(DenseMatrix.horzcat(_,_))
    blockPartitionMat.t.toArray.toList
  }

  def getBlockPartition(rowPartition: List[Int], colPartition: List[Int]): List[Int] = {
    val n = rowPartition.length
    val p = colPartition.length
    val blockBiPartition: List[(Int, Int)] = DenseMatrix.tabulate[(Int, Int)](n,p)(
      (i, j) => (rowPartition(i), colPartition(j))).toArray.toList
    val mapBlockBiIndexToBlockNum = blockBiPartition.distinct.zipWithIndex.toMap
    blockBiPartition.map(mapBlockBiIndexToBlockNum(_))
  }

  def combineRedundantAndCorrelatedColPartitions(redundantColPartition: List[Int],
                                                 correlatedColPartitions: List[List[Int]]): List[Int] = {
    val reducedCorrelatedPartition = correlatedColPartitions.reduce(_++_)
    val orderedRedundantColPartition = redundantColPartition.zipWithIndex.sortBy(_._1)

    relabel(orderedRedundantColPartition.indices.map(j => {
      val vj  = orderedRedundantColPartition(j)._1
      val idx = orderedRedundantColPartition(j)._2
      val wj  = reducedCorrelatedPartition(j)
      (idx, vj, wj)
    }).sortBy(_._1).map(c => (c._2, c._3)).toList)
  }

  def getSizeAndSumByBlock(data: RDD[((Int, Int), Array[DenseVector[Double]])]): RDD[((Int, Int), (DenseVector[Double], Int))] = {
    data
      .map(r => (r._1, (r._2.reduce(_+_), r._2.length)))
      .reduceByKey((a,b) => (a._1 + b._1, a._2+b._2))
  }

  private def getDataByBlock(data: RDD[(Int, Array[DenseVector[Double]], List[Int])],
                             partitionPerColBc: Broadcast[DenseVector[Int]],
                             KVec: List[Int]): RDD[((Int, Int), Array[DenseVector[Double]])] = {
    data.flatMap(row => {
      KVec.indices.map(l => {
        val rowDv = DenseVector(row._2)
        val rowInColumn = rowDv(partitionPerColBc.value:==l)
        ((l, row._3(l)), rowInColumn.toArray)
      })
    }).cache()

  }

  private def getCovarianceMatrices(dataPerColumnAndRow: RDD[((Int, Int), Array[DenseVector[Double]])],
                                    meanByBlock: Map[(Int, Int), DenseVector[Double]],
                                    sizeBlockMap: Map[(Int, Int), Int],
                                    KVec: List[Int],
                                    fullCovariance: Boolean=true): immutable.IndexedSeq[immutable.IndexedSeq[DenseMatrix[Double]]] = {
    val covFunction = unitCovFunc(fullCovariance)

    val sumCentered = dataPerColumnAndRow.map(r => {
      (r._1, r._2.map(v => covFunction(v - meanByBlock(r._1))).reduce(_+_))
    }).reduceByKey(_+_).collect().toMap
    KVec.indices.map(l => {
      (0 until KVec(l)).map(k => {
        sumCentered(l,k)/(sizeBlockMap(l,k).toDouble-1)
      })
    })
  }

  def getMeansAndCovariances(data: RDD[(Int, Array[DenseVector[Double]], List[Int])],
                             colPartition: Broadcast[DenseVector[Int]],
                             KVec: List[Int],
                             fullCovariance: Boolean,
                             verbose: Boolean) = {
    val dataByBlock: RDD[((Int, Int), Array[DenseVector[Double]])] = getDataByBlock(data, colPartition, KVec)
    val sizeAndSumBlock = getSizeAndSumByBlock(dataByBlock)
    val sizeBlock = sizeAndSumBlock.map(r => (r._1, r._2._2))
    val sizeBlockMap = sizeBlock.collect().toMap
    if(verbose){Common.Tools.prettyPrint(sizeBlockMap)}
    val meanByBlock: Map[(Int, Int), DenseVector[Double]] = sizeAndSumBlock.map(r => (r._1, r._2._1 / r._2._2.toDouble)).collect().toMap
    val covMat = getCovarianceMatrices(dataByBlock, meanByBlock, sizeBlockMap, KVec, fullCovariance)
    val listMeans = KVec.indices.map(l => {
      (0 until KVec(l)).map(k => {meanByBlock(l, k)}).toList
    }).toList
    (listMeans, covMat, sizeBlock, sizeBlockMap)
  }

  def getLoadingsAndVarianceExplained(covarianceMatrix: DenseMatrix[Double],
                                      nMaxEigenValues: Int= 3,
                                      thresholdVarExplained: Double=0.999999): (DenseMatrix[Double], DenseVector[Double]) = {

    require(thresholdVarExplained >= 0 & thresholdVarExplained < 1, "thresholdVarExplained should be >= 0 and <= 1")

    val (sortedEigenVectors, sortedEigenValues) = getLoadingsAndEigenValues(covarianceMatrix)

    val normalizedEigenValues = sortedEigenValues/sum(sortedEigenValues)
    val cumulatedVarExplained = normalizedEigenValues.toArray.map{var s = 0D; d => {s += d; s}}
    val idxMaxVarianceExplained = cumulatedVarExplained.toList.indexWhere(_> thresholdVarExplained)
    val idxKept = min(idxMaxVarianceExplained+1, nMaxEigenValues)
    val keptEigenVectors = sortedEigenVectors(::, 0 until idxKept)

    (keptEigenVectors.t, normalizedEigenValues.slice(0, idxKept))
  }

  def getLoadingsAndEigenValues(covarianceMatrix: DenseMatrix[Double]): (DenseMatrix[Double], DenseVector[Double]) = {

    val EigSym(eVal, eVec) = eigSym(covarianceMatrix)
    val sortedEigVal = DenseVector(eVal.toArray.sorted.reverse)
    val sortedEigVec = DenseMatrix((0 until eVec.rows).map(i => (eVec(::,i), eVal(i))).sortBy(-_._2).map(_._1):_*)
    (sortedEigVec.t, sortedEigVal)
  }


  def factorial(n: Double): Double = {
    if (n == 0) {1} else {n * factorial(n-1)}
  }

  def logFactorial(n: Double): Double = {
    if (n == 0) {0} else {log(n) + logFactorial(n-1)}
  }

  def round(m: DenseMatrix[Double], digits:Int): DenseMatrix[Double] = {
    m.map(round(_,digits))
  }

  def round(x: Double, digits:Int): Double = {
    val factor: Double = Math.pow(10,digits)
    Math.round(x*factor)/factor
  }

  def matrixToDataByCol(data: DenseMatrix[DenseVector[Double]]): List[List[DenseVector[Double]]] = {
    data(::,*).map(_.toArray.toList).t.toArray.toList
  }


  def getScores(estimatedPartition: List[Int], observedPartition: List[Int]) = {
    (adjustedRandIndex(estimatedPartition.toArray, observedPartition.toArray),
      randIndex(estimatedPartition.toArray, observedPartition.toArray),
      NormalizedMutualInformation.sum(estimatedPartition.toArray, observedPartition.toArray),
      estimatedPartition.distinct.length)
  }

}
