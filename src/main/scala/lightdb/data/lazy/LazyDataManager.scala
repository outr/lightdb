package lightdb.data.`lazy`

import lightdb.data.DataManager

import java.nio.ByteBuffer

class LazyDataManager[T] extends DataManager[LazyData[T]] {
  override def fromArray(array: Array[Byte]): LazyData[T] = ???

  override def toArray(value: LazyData[T]): Array[Byte] = ???
}

trait LazyData[T] {
  def data: T
//  def apply[F](field: Field[F, T]): F
}

trait DataPoint[T] {
  def `type`: DataType
  def length(offset: Int, bytes: ByteBuffer): Int
  def length(value: T): Int
  def get(offset: Int, length: Int, bytes: ByteBuffer): T
  def set(offset: Int, value: T, bytes: ByteBuffer): Unit
}

object IntDataPoint extends DataPoint[Int] {
  override def `type`: DataType = DataType.Fixed
  override def length(offset: Int, bytes: ByteBuffer): Int = 4
  override def length(value: Int): Int = 4
  override def get(offset: Int, length: Int, bytes: ByteBuffer): Int = bytes.getInt(offset)
  override def set(offset: Int, value: Int, bytes: ByteBuffer): Unit = bytes.putInt(offset, value)
}

object StringDataPoint extends DataPoint[String] {
  override def `type`: DataType = DataType.Variable
  override def length(offset: Int, bytes: ByteBuffer): Int = bytes.getInt(offset)
  override def length(value: String): Int = value.length
  override def get(offset: Int, length: Int, bytes: ByteBuffer): String = {
    val array = new Array[Byte](length)
    bytes.get(array, offset, length)
    new String(array, "UTF-8")
  }
  override def set(offset: Int, value: String, bytes: ByteBuffer): Unit = {
    value.getBytes("UTF-8").zipWithIndex.foreach {
      case (b, i) => bytes.put(offset + i, b)
    }
  }
}

trait CombinedDataPoint[T] extends DataPoint[T] {
  protected def map: Map[String, DataPoint[Any]]
  protected def value2Map(value: T): Map[String, Any]
  protected def map2Value(map: Map[String, Any]): T

  private lazy val points = map.toList.sortBy(_._1).map(_._2)
  private lazy val reverseMap = map.map(t => t._2 -> t._1)

  override lazy val `type`: DataType = if (points.exists(_.`type` == DataType.Variable)) DataType.Variable else DataType.Fixed
  override def length(offset: Int, bytes: ByteBuffer): Int = points.foldLeft(offset)((sum, point) => {
    sum + point.length(sum, bytes)
  })
  override def length(value: T): Int = {
    val data = value2Map(value)
    points.foldLeft(0)((sum, point) => sum + point.length(data(reverseMap(point))))
  }
  override def get(offset: Int, length: Int, bytes: ByteBuffer): T = {
    var data = Map.empty[String, Any]
    var index = offset
//    points.foreach { point =>
//      val name = reverseMap(point)
//      data += name -> point.get(index, )
//    }
    ???
  }
  override def set(offset: Int, value: T, bytes: ByteBuffer): Unit = ???
}

sealed trait DataType

object DataType {
  case object Fixed extends DataType
  case object Variable extends DataType
}

/*
object Test {
  def main(args: Array[String]): Unit = {
    val map = Map(
      "name" -> StringDataPoint,
      "age" -> IntDataPoint
    )
    val reverseMap = map.map(t => t._2 -> t._1)
    val (fixed, variable) = map.values.toList.map(_.asInstanceOf[DataPoint[Any]]).partition(_.`type` == DataType.Fixed)
    val data = Map(
      "name" -> "Matt Hicks",
      "age" -> 41
    )
    val fixedLength = fixed.foldLeft(0)((sum, point) => sum + point.length(data(reverseMap(point))))
    val variableLength = variable.foldLeft(0)((sum, point) => sum + point.length(data(reverseMap(point))))
    val variableLengthsLength = variable.length * 4
    val length = variableLengthsLength + fixedLength + variableLength
    scribe.info(s"Fixed Length: $fixedLength, Variable Length: $variableLength, Variable Lengths Length: $variableLengthsLength")
    scribe.info(s"Length: $length")
  }
}*/
