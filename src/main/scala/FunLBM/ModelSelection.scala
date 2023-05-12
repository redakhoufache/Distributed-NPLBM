package FunLBM

import Common.Tools._
import breeze.linalg.{DenseMatrix, DenseVector, argmax}

object ModelSelection {

  def gridSearch(data: DenseMatrix[DenseVector[Double]],
                 EMMethod: String= "SEMGibbs",
                 rangeRow:List[Int],
                 rangeCol:List[Int],
                 verbose: Boolean = false,
                 nConcurrentEachTest:Int=1,
                 nTryMaxPerConcurrent:Int=30): Map[String,Product] = {

    var latentBlock = new LatentBlock()
    val gridRange: List[(Int, Int)] = (rangeRow cross rangeCol).toList
    val allRes = gridRange.map(KL => {
      if(verbose) {println()
        println(">>>>> LBM Grid Search try: (K:"+KL._1.toString+", L:"+KL._2.toString+")")}
      latentBlock.setK(KL._1).setL(KL._2)
      latentBlock.run(data,
        EMMethod,
        nConcurrent = nConcurrentEachTest,
        nTryMaxPerConcurrent = nTryMaxPerConcurrent,
        verbose=verbose)
    })

    val Loglikelihoods: DenseVector[Double] = DenseVector(allRes.map(_("LogLikelihood")
      .asInstanceOf[List[Double]].last).toArray)
    val ICLs: DenseVector[Double] = DenseVector(allRes.map(_("ICL")
      .asInstanceOf[List[Double]].last).toArray)

    if(verbose) {
      println()
      gridRange.indices.foreach(i => {
        println("("+gridRange(i)._1+", "+ gridRange(i)._2+"), Loglikelihood: ", Loglikelihoods(i)+", ICL: "+ICLs(i))
      })
    }

    allRes(argmax(ICLs))
  }


}
