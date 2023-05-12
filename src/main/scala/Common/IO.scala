package Common

import java.io.{BufferedWriter, FileWriter}

import breeze.linalg.{*, DenseMatrix, DenseVector}
import breeze.stats.distributions.MultivariateGaussian
import com.opencsv.CSVWriter

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.{Failure, Try}

object IO {

  def readDataSet(path: String): List[List[Double]] = {
    val lines = Source.fromFile(path).getLines.toList.drop(1)
    lines.indices.map(seg => {
      lines(seg).drop(1).dropRight(1).split(";").toList.map(string => string.split(":")(1).toDouble)
    }).toList
  }

  def readDenseMatrixDvDouble(path: String): DenseMatrix[DenseVector[Double]] = {
    val lines = Source.fromFile(path).getLines.toList.drop(1)
    DenseMatrix(lines.map(line =>  {
      val elementList = line.drop(1).dropRight(1).split("\",\"").toList
      elementList.map(string => DenseVector(string.split(":").map(_.toDouble)))
    }):_*)
  }

  def addIndex(content: List[List[String]]): List[List[String]] =
    content.foldLeft((1, List.empty[List[String]])){
      case ((serial: Int, acc: List[List[String]]), value: List[String]) =>
        (serial + 1, (serial.toString +: value) +: acc)
    }._2.reverse

  def writeMatrixStringToCsv(fileName: String, Matrix: DenseMatrix[String], append: Boolean = false): Unit = {
    val rows : List[List[String]] = Matrix(*, ::).map(dv => dv.toArray.toList).toArray.toList
    writeCsvFile(fileName, addIndex(rows), append=append)
  }

  def writeMatrixDoubleToCsv(fileName: String, Matrix: DenseMatrix[Double], withHeader:Boolean=true): Unit = {
    val header: List[String] = List("id") ++ (0 until Matrix.cols).map(_.toString).toList
    val rows : List[List[String]] = Matrix(*, ::).map(dv => dv.toArray.map(_.toString).toList).toArray.toList
    if(withHeader){
      writeCsvFile(fileName, addIndex(rows), header)
    } else {
      writeCsvFile(fileName, addIndex(rows))
    }
  }

  def writeMatrixDvDoubleToCsv(fileName: String, Matrix: DenseMatrix[DenseVector[Double]], withHeader:Boolean=true): Unit = {
    val header: List[String] = (0 until Matrix.cols).map(_.toString).toList

    val rows : List[List[String]] = Matrix(*, ::).map(dv => dv.toArray.map(_.toArray.mkString(":")).toList).toArray.toList
    if(withHeader){
      writeCsvFile(fileName, rows, header)
    } else {
      writeCsvFile(fileName, rows)
    }
  }

  def writeMatrixIntToCsv(fileName: String, Matrix: DenseMatrix[Int], withHeader:Boolean=true): Unit = {
    val header: List[String] = List("id") ++ (0 until Matrix.cols).map(_.toString).toList
    val rows : List[List[String]] = Matrix(*, ::).map(dv => dv.toArray.map(_.toString).toList).toArray.toList
    if(withHeader){
      writeCsvFile(fileName, addIndex(rows), header)
    } else {
      writeCsvFile(fileName, addIndex(rows))
    }
  }

  def writeCsvFile(fileName: String,
                   rows: List[List[String]],
                   header: List[String] = List.empty[String],
                   append:Boolean=false
                  ): Try[Unit] =
  {
    val content = if(header.isEmpty){rows} else {header +: rows}
    Try(new CSVWriter(new BufferedWriter(new FileWriter(fileName, append)))).flatMap((csvWriter: CSVWriter) =>
      Try{
        csvWriter.writeAll(
          content.map(_.toArray).asJava
        )
        csvWriter.close()
      } match {
        case f @ Failure(_) =>
          // Always return the original failure.  In production code we might
          // define a new exception which wraps both exceptions in the case
          // they both fail, but that is omitted here.
          Try(csvWriter.close()).recoverWith{
            case _ => f
          }
        case success =>
          success
      }
    )
  }

  def writeGaussianComponentsParameters(pathOutput: String, components: List[List[MultivariateGaussian]]): Unit = {
    val outputContent = components.map(gaussianList => {
      gaussianList.map(G =>
        List(
          G.mean.toArray.mkString(":"),
          G.covariance.toArray.mkString(":"))).reduce(_++_)
    })
    Common.IO.writeCsvFile(pathOutput, Common.IO.addIndex(outputContent))
  }

}
