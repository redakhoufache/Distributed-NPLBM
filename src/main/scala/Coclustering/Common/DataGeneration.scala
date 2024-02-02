package Coclustering.Common
import Coclustering.Common.ProbabilisticTools._
import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.{MultivariateGaussian, RandBasis}
import java.io.{FileOutputStream, PrintStream}
import scala.util.Random

object DataGeneration  {
  def randomLBMDataGeneration(modes: List[List[DenseVector[Double]]],
                              covariances: List[List[DenseMatrix[Double]]],
                              sizeClusterRow: List[Int],
                              sizeClusterCol: List[Int],
                              shuffle: Boolean = false,
                              mixingProportion: Double = 0D): DenseMatrix[DenseVector[Double]] = {

    val modeLengths = modes.map(_.length)
    val K = modeLengths.head
    val covLengths = covariances.map(_.length)
    val L = modes.length

    require(modeLengths.forall(_ == modeLengths.head), "In LBM case, every column must have the same number of modes")
    require(covLengths.forall(_ == covLengths.head), "In LBM case, every column must have the same number of covariances matrices")
    require(K == covLengths.head, s"modes and covariances K do not match ${K} == ${covLengths.head}")
    require(modes.length == covariances.length, "modes and covariances L do not match")
    require(sizeClusterRow.length == K,s"${sizeClusterRow.length}->$K")
    require(sizeClusterCol.length == L,s"${sizeClusterCol.length}->$L")

    val sizeClusterRowEachColumn = List.fill(L)(sizeClusterRow)
    val dataPerBlock: List[List[DenseMatrix[DenseVector[Double]]]] = generateDataPerBlock(
      modes,
      covariances,
      sizeClusterRowEachColumn,
      sizeClusterCol,
      mixingProportion)
    val data: DenseMatrix[DenseVector[Double]] = doubleReduce(dataPerBlock)
    applyConditionalShuffleData(data, shuffle)
  }

  def generateDataPerBlock(modes: List[List[DenseVector[Double]]],
                           covariances: List[List[DenseMatrix[Double]]],
                           sizeClusterRow: List[List[Int]],
                           sizeClusterCol: List[Int],
                           mixingProportion: Double=0D,
                           seed: Option[Int] = None): List[List[DenseMatrix[DenseVector[Double]]]]={

    val actualSeed = seed match {
      case Some(s) => s;
      case None => scala.util.Random.nextInt()
    }

    implicit val basis: RandBasis = RandBasis.withSeed(actualSeed)

    require(mixingProportion>=0D & mixingProportion <=1D)
    val L = modes.length
    val KVec = modes.map(_.length)
    val MGaussians = (0 until L).map(l => {
      modes(l).indices.map(k => {
        MultivariateGaussian(modes(l)(k),covariances(l)(k))
      })
    })
    modes.indices.map(l => {
      val K_l = modes(l).length
      modes(l).indices.map(k => {
        val dataList: Array[DenseVector[Double]] = MGaussians(l)(k).sample(sizeClusterRow(l)(k)*sizeClusterCol(l)).toArray

        val mixedData = dataList.map(data => {
          val isMixed = sampleWithSeed(List(1-mixingProportion, mixingProportion), 2)
          if (isMixed == 0){
            data
          } else {
            val newl = sample(L)
            val newk = sample(KVec(newl))
            MGaussians(newl)(newk).draw()
          }
        })

        DenseMatrix(mixedData).reshape(sizeClusterRow(l)(k),sizeClusterCol(l))
      }).toList
    }).toList
  }
  def doubleReduce(dataList: List[List[DenseMatrix[DenseVector[Double]]]]): DenseMatrix[DenseVector[Double]] = {
    dataList.indices.map(l => {
      dataList(l).indices.map(k_l => {
        dataList(l)(k_l)
      }).reduce(DenseMatrix.vertcat(_,_))
    }).reduce(DenseMatrix.horzcat(_,_))
  }
  def applyConditionalShuffleData(data: DenseMatrix[DenseVector[Double]], shuffle:Boolean): DenseMatrix[DenseVector[Double]]= {
    if(shuffle){
      val newRowIndex: List[Int] = Random.shuffle((0 until data.rows).toList)
      val newColIndex: List[Int] = Random.shuffle((0 until data.cols).toList)
      DenseMatrix.tabulate[DenseVector[Double]](data.rows,data.cols){ (i,j) => data(newRowIndex(i), newColIndex(j))}
    } else data
  }
  def generateLBMData(path: String): Unit = {

    val sizeClusterRow = List(50, 40, 10)
    val sizeClusterCol = List(40, 40, 20)
    val datasetName = "synthetic_100_100_9.csv"
    val modes = List(
      List(DenseVector(0.976), DenseVector(4.303), DenseVector(-1.527)),
      List(DenseVector(-2.33), DenseVector(0.834), DenseVector(8.5119)),
      List(DenseVector(5.563), DenseVector(9.572), DenseVector(-3.771)))

    val covariances = List(
      List(DenseMatrix(0.669), DenseMatrix(1.761), DenseMatrix(1.386)),
      List(DenseMatrix(1.851), DenseMatrix(1.079), DenseMatrix(1.944)),
      List(DenseMatrix(0.933), DenseMatrix(1.973), DenseMatrix(0.752)))

/*
    val sizeClusterRow = List(2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000)
    val sizeClusterCol = List(7, 7, 6)
    val datasetName = "synthetic_20000_20_30.csv"
    val modes = List(
      List(DenseVector(0.97627008), DenseVector(4.30378733), DenseVector(2.05526752),
        DenseVector(0.89766366), DenseVector(-1.52690401), DenseVector(2.91788226),
        DenseVector(-1.24825577), DenseVector(7.83546002), DenseVector(9.27325521), DenseVector(8.89337834)),
      List(DenseVector(-2.33116962), DenseVector(5.83450076), DenseVector(0.5778984),
        DenseVector(1.36089122), DenseVector(8.51193277), DenseVector(-8.57927884),
        DenseVector(-8.25741401), DenseVector(-9.59563205), DenseVector(6.65239691), DenseVector(0.43696644)),
      List(DenseVector(5.56313502), DenseVector(7.40024296), DenseVector(9.57236684),
        DenseVector(5.98317128), DenseVector(-0.77041275), DenseVector(5.61058353),
        DenseVector(-7.63451148), DenseVector(2.79842043), DenseVector(-7.13293425), DenseVector(-1.7067612)))
    val covariances = List(List(DenseMatrix(0.669336507796175), DenseMatrix(1.7612065703879582),
      DenseMatrix(1.3862592599171373), DenseMatrix(1.1238622175611195), DenseMatrix(1.2457670824447593),
      DenseMatrix(1.6931717416325167), DenseMatrix(1.5972692066111005), DenseMatrix(1.1159170518255292),
      DenseMatrix(1.219057950452216), DenseMatrix(0.8289552408204514)),
      List(DenseMatrix(1.8518144848803162), DenseMatrix(1.079276132540391),
        DenseMatrix(1.9446262792984377), DenseMatrix(0.8714880183032959),
        DenseMatrix(1.532140243991034), DenseMatrix(1.084828014988245),
        DenseMatrix(1.2964549237719827), DenseMatrix(0.5460550074747246),
        DenseMatrix(1.8643446504119405), DenseMatrix(1.876902873474071)),
      List(DenseMatrix(0.9333442461964505), DenseMatrix(1.9731924700985997),
        DenseMatrix(0.7524495254489219), DenseMatrix(0.9777400894782611),
        DenseMatrix(1.7316683965907), DenseMatrix(1.0062588416920026),
        DenseMatrix(1.9621619190951707), DenseMatrix(1.9494185295668427),
        DenseMatrix(1.8494073742054244), DenseMatrix(0.5954647897783252)))
*/
    val generatedData = randomLBMDataGeneration(modes = modes, covariances = covariances, sizeClusterRow = sizeClusterRow, sizeClusterCol = sizeClusterCol)

    val data = (0 until 100).par.map(i => {
                   (0 until 100).par.map(j => {
                      generatedData.valueAt(i, j)
                   }).toList
                }).toList
      System.setOut(new PrintStream(new FileOutputStream(s"$path/data/$datasetName")))
      System.out.println((0 until 100).map(_.toString).mkString(","))
    data.foreach(line =>
        System.out.println(line.map(cell => cell.valueAt(0).toString).mkString(","))
        )
  }
}