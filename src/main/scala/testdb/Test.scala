package testdb

import java.io.File

import com.oath.halodb.{HaloDB, HaloDBOptions}
import io.circe.{Json, Printer}
import io.youi.Unique

import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.matching.Regex

object Test {
  def main(args: Array[String]): Unit = {
    val opts = new HaloDBOptions
    opts.setBuildIndexThreads(8)

    val db = HaloDB.open("testdb", opts)
    try {
      db.put("Test1".bytes, "Hello, World!".bytes)
      db.put("Test2".bytes, "Goodbye, World!".bytes)

      println(s"Result: ${db.get("Test2".bytes).string}")

      db.newIterator().asScala.foreach { record =>
        println(s"Record: ${record.getKey.string} = ${record.getValue.string}")
      }
    } finally {
      db.close()
    }
  }

  def test(): Unit = {
    val iterator: DBIterator[User] = Database.users
      .limit(100)
      .offset(10)
      .filter(User.name === "Matt")
      .filter(User.age >= 21)
//      .filter(User.name fuzzy "mt")
//      .sort(User.age.desc)
      .query()
  }

  implicit class BytesExtras(val array: Array[Byte]) extends AnyVal {
    def string: String = new String(array, "UTF-8")
  }

  implicit class StringExtras(val s: String) extends AnyVal {
    def bytes: Array[Byte] = s.getBytes("UTF-8")
  }

  implicit class IntFieldExtras(val f: Field[Int]) extends AnyVal {
    def >=(value: Int): Filter = Filter.ValueComparison(f, Operator.GreaterOrEqual, value)
  }
}

// Create "core" for cross-platform no-dependencies code
// Create "database" for database code (JVM-only)

case class Id[T](collection: String, value: String = Unique(length = 32))

object Id {
  private val IdRegex: Regex = """(.+)[/](.+)""".r

  def toString[T](id: Id[T]): String = s"${id.collection}/${id.value}"
  def fromString[T](s: String): Id[T] = s match {
    case IdRegex(collection, value) => Id[T](collection, value)
  }
}

trait Document[D <: Document[D]] {
  val id: Id[D]
}

trait DocumentModel[D <: Document[D]] {
  def field[T](name: String, stored: Boolean = true): Field[T] = Field[T](name, stored)
}

case class Field[T](name: String, stored: Boolean) {
  def ===(value: T): Filter = Filter.ValueComparison(this, Operator.Equals, value)
}

case class User(name: String, age: Int, id: Id[User]) extends Document[User]

object User extends DocumentModel[User] {
  val name: Field[String] = field("name")
  val age: Field[Int] = field("age")
}

sealed trait Filter

object Filter {
  case class ValueComparison[T](field: Field[T], operator: Operator, value: T) extends Filter
}

sealed trait Operator

object Operator {
  case object Equals extends Operator
  case object Greater extends Operator
  case object GreaterOrEqual extends Operator
  case object Less extends Operator
  case object LessOrEqual extends Operator
}

trait Sort {
  def fieldName: String
  def reverse: Boolean
}

trait QueryBuilder[T] {
  def limit(limit: Int): QueryBuilder[T]
  def offset(offset: Int): QueryBuilder[T]
  def filter(filter: Filter): QueryBuilder[T]
  def sort(sort: Sort): QueryBuilder[T]
  def query(): DBIterator[T]
}

trait DBIterator[T] extends Iterator[T] {
  def total: Int
  def position: Int
}

trait Collection[D <: Document[D]] extends QueryBuilder[D] {
  def model: DocumentModel[D]
}

trait DB {
  val directory: Option[File]

  lazy val store: Store = directory match {
    case Some(dir) => new HaloDBStore(new File(dir, "store"))
    case None => new MapStore
  }

  protected def collection[D <: Document[D]]: Collection[D] = ???
}

object Database extends DB {
  override val directory: Option[File] = None

  val users: Collection[User] = collection[User]
}

trait Store {
  def get(key: String): Option[Json]
  def apply(key: String): Json
  def update(key: String, value: Json): Unit
  def close(): Unit
}

class HaloDBStore(directory: File) extends Store {
  private lazy val options: HaloDBOptions = {
    val o = new HaloDBOptions
    o.setBuildIndexThreads(8)
    o
  }
  private lazy val db = HaloDB.open(directory, options)

  override def get(key: String): Option[Json] = Try(apply(key)).toOption

  override def apply(key: String): Json = {
    val keyBytes = key.getBytes("UTF-8")
    val valueBytes = db.get(keyBytes)
    val valueString = new String(valueBytes, "UTF-8")
    io.circe.parser.parse(valueString) match {
      case Left(failure) => throw failure
      case Right(json) => json
    }
  }

  override def update(key: String, value: Json): Unit = {
    val keyBytes = key.getBytes("UTF-8")
    val valueString = value.printWith(Printer.spaces2)
    val valueBytes = valueString.getBytes("UTF-8")
    db.put(keyBytes, valueBytes)
  }

  override def close(): Unit = db.close()
}

class MapStore extends Store {
  private var map = Map.empty[String, Json]

  override def get(key: String): Option[Json] = map.get(key)

  override def apply(key: String): Json = map(key)

  override def update(key: String, value: Json): Unit = synchronized {
    map += key -> value
  }

  override def close(): Unit = synchronized {
    map = Map.empty
  }
}