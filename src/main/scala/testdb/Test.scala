package testdb

import com.oath.halodb.{HaloDB, HaloDBOptions}
import com.outr.lucene4s.query.QueryBuilder
import io.youi.Unique

import scala.jdk.CollectionConverters._
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
      .filter(User.name fuzzy "mt")
      .sort(User.age.desc)
      .query()
  }

  implicit class BytesExtras(val array: Array[Byte]) extends AnyVal {
    def string: String = new String(array, "UTF-8")
  }

  implicit class StringExtras(val s: String) extends AnyVal {
    def bytes: Array[Byte] = s.getBytes("UTF-8")
  }
}

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
  def field[T](name: String): Field[T] = new Field[T](name)
}

case class Filter()

class Field[T](val name: String) extends AnyVal

object Field {
  def apply[T](name: String): Field[T] = new Field[T](name)
}

case class User(name: String, age: Int, id: Id[User]) extends Document[User]

object User extends DocumentModel[User] {
  val name: Field[String] = field("name")
  val age: Field[Int] = field("age")
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
  protected def collection[D <: Document[D]]: Collection[D] = ???
}

object Database extends DB {
  val users: Collection[User] = collection[User]
}