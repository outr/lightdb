package testdb

import io.youi.Unique

import scala.util.matching.Regex

case class Id[T](collection: String, value: String = Unique(length = 32)) {
  lazy val bytes: Array[Byte] = toString.getBytes("UTF-8")

  override def toString: String = s"$collection/$value"
}

object Id {
  private val IdRegex: Regex = """(.+)[/](.+)""".r

  def toString[T](id: Id[T]): String = s"${id.collection}/${id.value}"

  def fromString[T](s: String): Id[T] = s match {
    case IdRegex(collection, value) => Id[T](collection, value)
  }
}