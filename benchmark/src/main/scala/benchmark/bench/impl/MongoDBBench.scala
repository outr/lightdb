package benchmark.bench.impl

import benchmark.bench.Bench
import com.mongodb.client.MongoClients
import com.mongodb.client.model.{Filters, Indexes}
import org.bson.Document

import scala.jdk.CollectionConverters._

object MongoDBBench extends Bench {
  override def name: String = "MongoDB"

  private lazy val client = MongoClients.create()
  private lazy val db = client.getDatabase("bench")
  private lazy val people = db.getCollection("people")

  override def init(): Unit = {
    people.createIndex(Indexes.ascending("age"))
  }

  override protected def insertRecords(iterator: Iterator[P]): Unit = {
    iterator
      .map { p =>
        new Document(Map[String, AnyRef](
          "id" -> p.id,
          "name" -> p.name,
          "age" -> Integer.valueOf(p.age)
        ).asJava)
      }
      .grouped(5_000)
      .foreach { seq =>
        people.insertMany(seq.asJava)
      }
  }

  override protected def streamRecords(f: Iterator[P] => Unit): Unit = {
    val iterator = people.find().iterator().asScala.map { document =>
      P(
        id = document.getString("id"),
        name = document.getString("name"),
        age = document.getInteger("age").intValue()
      )
    }
    f(iterator)
  }

  override protected def getEachRecord(idIterator: Iterator[String]): Unit = {
    idIterator.foreach { id =>
      val document = people.find(Filters.eq("id", id)).first()
      val p = P(
        id = document.getString("id"),
        name = document.getString("name"),
        age = document.getInteger("age").intValue()
      )
      if (p.id != id) {
        scribe.warn(s"${p.id} was not $id")
      }
    }
  }

  override protected def searchEachRecord(ageIterator: Iterator[Int]): Unit = {
    ageIterator.foreach { age =>
      val document = people.find(Filters.eq("age", age)).first()
      val p = P(
        id = document.getString("id"),
        name = document.getString("name"),
        age = document.getInteger("age").intValue()
      )
      if (p.age != age) {
        scribe.warn(s"${p.age} was not $age")
      }
    }
  }

  override protected def searchAllRecords(f: Iterator[P] => Unit): Unit = streamRecords(f)

  override def size(): Long = 0L

  override def dispose(): Unit = client.close()
}
