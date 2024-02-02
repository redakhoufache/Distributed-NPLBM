package Coclustering.Common
import breeze.linalg.DenseMatrix
import breeze.numerics.log
import smile.validation.{NormalizedMutualInformation, adjustedRandIndex}

import scala.collection.mutable.ListBuffer

object Tools extends java.io.Serializable {



  def partitionToOrderedCount(partition: List[Int]): List[Int] = {
    partition.groupBy(identity).mapValues(_.size).toList.sortBy(_._1).map(_._2)
  }

  def printTime(t0:Long, stepName: String, verbose:Boolean = true): Long={
    if(verbose){
      println(stepName.concat(" step duration: ").concat(((System.nanoTime - t0)/1e9D ).toString))
    }
    System.nanoTime
  }

  def roundMat(m: DenseMatrix[Double], digits:Int=0): DenseMatrix[Double] = {
    m.map(round(_,digits))
  }

  def getBlockPartition(rowPartition: List[Int], colPartition: List[Int]): List[Int] = {
    val n = rowPartition.length
    val p = colPartition.length
    val blockBiPartition: List[(Int, Int)] = DenseMatrix.tabulate[(Int, Int)](n,p)(
      (i, j) => (rowPartition(i), colPartition(j))).toArray.toList
    val mapBlockBiIndexToBlockNum = blockBiPartition.distinct.zipWithIndex.toMap
    blockBiPartition.map(mapBlockBiIndexToBlockNum(_))
  }

  def factorial(n: Double): Double = {
    if (n == 0) {1} else {n * factorial(n-1)}
  }

  def logFactorial(n: Double): Double = {
    if (n == 0) {0} else {log(n) + logFactorial(n-1)}
  }

  def round(x: Double, digits:Int): Double = {
    val factor: Double = Math.pow(10,digits)
    Math.round(x*factor)/factor
  }

  def getScores(estimatedPartition: List[Int], observedPartition: List[Int]): (Double, Double, Int) = {
    (adjustedRandIndex(estimatedPartition.toArray, observedPartition.toArray),
      NormalizedMutualInformation.sum(estimatedPartition.toArray, observedPartition.toArray),
      estimatedPartition.distinct.length)
  }

  def blockPartition(rowPartition: List[Int], colPartition: List[Int]): List[Int] = {
    val N=rowPartition.size
    val P=colPartition.size
    var result:ListBuffer[Int] =List.fill(N*P)(0).to[ListBuffer]
    val p = colPartition.size
    rowPartition.indices.map(i => {
      colPartition.indices.map(j => {
        result.update(i*P+j,rowPartition(i)*p+colPartition(j))
      })
    })
    result.toList
  }
}
