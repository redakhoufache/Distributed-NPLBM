package DAVID.Common

import DAVID.Common.ProbabilisticTools._
import DAVID.Common.Tools.allEqual
import breeze.linalg.{DenseMatrix, DenseVector, diag}
import breeze.stats.distributions.{MultivariateGaussian, RandBasis}

import java.io.{FileOutputStream, PrintStream}
import scala.util.Random

object DataGeneration  {

  def randomMixture(modes: List[DenseVector[Double]],
                              covariances: List[DenseMatrix[Double]],
                              sizeCluster: List[Int],
                              shuffle: Boolean = false): List[DenseVector[Double]] = {

    require(modes.length == covariances.length, "modes and covariances lengths do not match")
    require(modes.length == sizeCluster.length, "sizeCluster and modes lengths do not match")
    val K = modes.length

    val data = (0 until K).map(k => {
      MultivariateGaussian(modes(k), covariances(k)).sample(sizeCluster(k))
    }).reduce(_++_).toList
    if(shuffle){Random.shuffle(data)} else data
  }

  def randomFunLBMDataGeneration(prototypes: List[List[List[Double]=> List[Double]]],
                                 sigma: Double,
                                 sizeClusterRow: List[Int],
                                 sizeClusterCol: List[Int],
                                 shuffle: Boolean=true,
                                 mixingProportion: Double = 0D,
                                 seed: Option[Int] = None):
  List[(Int, Int, List[Double], List[Double])] = {

    require(sizeClusterRow.length == prototypes.head.length)
    require(sizeClusterCol.length == prototypes.length)
    val K = prototypes.head.length
    val L = prototypes.length

    val length = 1000
    val indices:List[Double] = (1 to length).map(_/length.toDouble).toList

    val modes = (0 until L).map(l => {(0 until K).map(k => {DenseVector(prototypes(l)(k)(indices).toArray)}).toList}).toList
    val covariances = (0 until L).map(_ => {(0 until K).map(_ => {diag(DenseVector(Array.fill(indices.length){sigma}))}).toList}).toList

    val sizeClusterRowEachColumn = List.fill(L)(sizeClusterRow)
    val dataPerBlock = generateDataPerBlock(modes, covariances, sizeClusterRowEachColumn, sizeClusterCol, mixingProportion, seed)
    val data: DenseMatrix[DenseVector[Double]] = doubleReduce(dataPerBlock)
    val dataShuffled = applyConditionalShuffleData(data, shuffle)
    (0 until data.rows).map(i => {(0 until data.cols).map(j => {(i, j, indices, dataShuffled(i,j).toArray.toList)}).toList}).reduce(_++_)
  }

  def randomFunCLBMDataGeneration(prototypes: List[List[List[Double]=> List[Double]]],
                                  sigma: Double,
                                  sizeClusterRow: List[List[Int]],
                                  sizeClusterCol: List[Int],
                                  shuffle: Boolean=true,
                                  mixingProportion: Double=0D):
  List[(Int, Int, List[Double], List[Double])] = {

    require(allEqual(prototypes.map(_.length), sizeClusterRow.map(_.length)), "SizeClusteRow and prototype lists lengths differ")
    require(sizeClusterCol.length == prototypes.length, "sizeClusterCol and prototype lengths differ")
    require(sizeClusterRow.map(_.sum == sizeClusterRow.head.sum).forall(identity), "Each sizeClusterRow list should sum up to the same number (= n, the number of observations)")

    val length = 100
    val indices:List[Double] = (1 to length).map(_/length.toDouble).toList

    val modes = prototypes.indices.map(l => {prototypes(l).indices.map(k => {DenseVector(prototypes(l)(k)(indices).toArray)}).toList}).toList
    val covariances = prototypes.indices.map(l => {prototypes(l).indices.map(_ => {
      diag(DenseVector(Array.fill(indices.length){sigma}))}).toList}).toList

    val dataPerBlock = generateDataPerBlock(modes, covariances, sizeClusterRow, sizeClusterCol, mixingProportion)
    val data: DenseMatrix[DenseVector[Double]] = doubleReduce(dataPerBlock)
    val dataShuffled = applyConditionalShuffleData(data, shuffle)
    (0 until data.rows).map(i => {(0 until data.cols).map(j => {(i, j, indices, dataShuffled(i,j).toArray.toList)}).toList}).reduce(_++_)
  }

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

  def randomLBMDataGeneration(mvGaussian: List[List[MultivariateGaussian]],
                              sizeClusterRow: List[Int],
                              sizeClusterCol: List[Int],
                              shuffle: Boolean,
                              mixingProportion: Double): DenseMatrix[DenseVector[Double]] = {

    val mvGaussisnLengths = mvGaussian.map(_.length)
    val K = mvGaussian.head.length
    val L = mvGaussian.length

    require(mvGaussisnLengths.forall(_ == K), "In LBM case, every column must have the same number of modes")
    require(sizeClusterRow.length == K)
    require(sizeClusterCol.length == L)

    val sizeClusterRowEachColumn = List.fill(L)(sizeClusterRow)
    val dataPerBlock: List[List[DenseMatrix[DenseVector[Double]]]] = generateDataPerBlock(
      mvGaussian,
      sizeClusterRowEachColumn,
      sizeClusterCol,
      mixingProportion,
      None)
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

  def generateDataPerBlock(mvGaussian: List[List[MultivariateGaussian]],
                           sizeClusterRow: List[List[Int]],
                           sizeClusterCol: List[Int],
                           mixingProportion: Double,
                           seed: Option[Int]): List[List[DenseMatrix[DenseVector[Double]]]]={

    val actualSeed = seed match {
      case Some(s) => s;
      case None => scala.util.Random.nextInt()
    }

    implicit val basis: RandBasis = RandBasis.withSeed(actualSeed)

    require(mixingProportion>=0D & mixingProportion <=1D)
    val L = mvGaussian.length
    val KVec = mvGaussian.map(_.length)

    mvGaussian.indices.map(l => {
      val K_l = KVec(l)
      mvGaussian(l).indices.map(k => {
        val dataList: Array[DenseVector[Double]] = mvGaussian(l)(k).sample(sizeClusterRow(l)(k)*sizeClusterCol(l)).toArray

        val mixedData = dataList.map(data => {
          val isMixed = sampleWithSeed(List(1-mixingProportion, mixingProportion), 2)
          if (isMixed == 0){
            data
          } else {
            val newl = sample(L)
            val newk = sample(KVec(newl))
            mvGaussian(newl)(newk).draw()
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
  def generate_20k_10k_10K_3L(path:String): Unit = {
    val sizeClusterRows = List(
      List(2045, 2016, 1959, 2049, 1973, 2009, 2026, 1955, 1984, 1984),
      List(4032, 3969, 3987, 3988, 3873, 4056, 4059, 4048, 4111, 3877),
      List(6014, 6084, 6075, 5881, 6092, 5903, 5917, 5933, 6041, 6060),
      List(8036, 7960, 8111, 8026, 7943, 8004, 7967, 7979, 8020, 7954),
      List(10054, 9959, 10009, 9906, 10093, 9996, 9848, 10114, 10082, 9939)
    )
    val sizeClusterCols = List(
      List(16, 25, 19),
      List(19, 20, 21),
      List(20, 25, 15),
      List(18, 20, 22),
      List(15, 25, 20)
    )
    val datasetNames = List(
      "synthetic_20000_60_30.csv",
      "synthetic_40000_60_30.csv",
      "synthetic_60000_60_30.csv",
      "synthetic_80000_60_30.csv",
      "synthetic_100000_60_30.csv"
    )
    val modes10k3L = List(
      List(DenseVector(0.97627008), DenseVector(4.30378733), DenseVector(2.05526752),
        DenseVector(0.89766366), DenseVector(-1.52690401), DenseVector(2.91788226),
        DenseVector(-1.24825577), DenseVector(7.83546002), DenseVector(9.27325521), DenseVector(8.89337834)),
      List(DenseVector(-2.33116962), DenseVector(5.83450076), DenseVector(0.5778984),
        DenseVector(1.36089122), DenseVector(8.51193277), DenseVector(-8.57927884),
        DenseVector(-8.25741401), DenseVector(-9.59563205), DenseVector(6.65239691), DenseVector(0.43696644)),
      List(DenseVector(5.56313502), DenseVector(7.40024296), DenseVector(9.57236684),
        DenseVector(5.98317128), DenseVector(-0.77041275), DenseVector(5.61058353),
        DenseVector(-7.63451148), DenseVector(2.79842043), DenseVector(-7.13293425), DenseVector(-1.7067612)))
    val covariances10K3L = List(List(DenseMatrix(0.669336507796175), DenseMatrix(1.7612065703879582),
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
    (0 until 5).map(index_dataset=> {
      val data_generated = randomLBMDataGeneration(modes = modes10k3L,
        covariances = covariances10K3L,
        sizeClusterRow = sizeClusterRows(index_dataset),
        sizeClusterCol = sizeClusterCols(index_dataset))
      /*val nLaunches = 10*/
      val data10K3L = (0 until 20000 + (index_dataset * 20000)).par.map(i => {
        (0 until 60).par.map(j => {
          data_generated.valueAt(i, j)
        }).toList
      }).toList
      System.setOut(new PrintStream(
        new FileOutputStream(s"$path/data/${datasetNames(index_dataset)}"))
      )
      System.out.println((0 until 60).map(_.toString).mkString(","))
      data10K3L.map(line=>
        System.out.println(line.map(cell=>cell.valueAt(0).toString).mkString(","))
      )
    })

  }
  def generate_reda(path:String): Unit = {
    val modes: List[List[DenseVector[Double]]] = List(
      List(DenseVector(-10.0), DenseVector(-15.0), DenseVector(5.0), DenseVector(0.0), DenseVector(10.0), DenseVector(-5.0), DenseVector(20.0), DenseVector(2.5), DenseVector(-2.5), DenseVector(7.5)),
      List(DenseVector(12.5), DenseVector(-12.5), DenseVector(17.5), DenseVector(-15.0), DenseVector(-7.5), DenseVector(25.0), DenseVector(30.0), DenseVector(-25.0), DenseVector(27.5), DenseVector(-30.0)),
      List(DenseVector(-27.5), DenseVector(40.0), DenseVector(45.0), DenseVector(-45.0), DenseVector(32.5), DenseVector(-32.5), DenseVector(-50.0), DenseVector(-45.0), DenseVector(47.5), DenseVector(50.0))
    )
    val covariances: List[List[DenseMatrix[Double]]] = List(
      List(DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01)),
      List(DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01)),
      List(DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01), DenseMatrix(0.01))
    )
    val n: Int = 2000
    val p: Int = 300
    val K: Int = 10
    val L: Int = 3
    val sizeClusterRow: List[Int] = List.fill(K)(n / K)
    val sizeClusterCol: List[Int] = List.fill(L)(p / L)
    println(s"sizeClusterRow=$sizeClusterRow")
    println(s"sizeClusterCol=$sizeClusterCol")
    val data_generated = randomLBMDataGeneration(modes = modes,
      covariances = covariances,
      sizeClusterRow = sizeClusterRow,
      sizeClusterCol = sizeClusterCol)
    val data10K3L=(0 until n).map(i=>{
      (0 until p).map(j=>{
        data_generated.valueAt(i, j)
      }).toList
    }).toList
    System.setOut(new PrintStream(
      new FileOutputStream(s"$path/data/synthetic_${n}_${p}_${(K*L)}.csv"))
    )
    System.out.println((0 until p).map(_.toString).mkString(","))
    data10K3L.map(line =>
      System.out.println(line.map(cell => cell.valueAt(0).toString).mkString(","))
    )
  }
}
