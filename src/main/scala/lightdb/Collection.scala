package lightdb

import cats.effect.{ExitCode, IO, IOApp}
import fabric.rw._
import lightdb.data.{DataManager, JsonDataManager}
import lightdb.store.HaloStore
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, StoredField, StringField, Field => LuceneField}
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, IndexableField, Term}
import org.apache.lucene.store.{ByteBuffersDirectory, Directory}

case class Collection[T](db: LightDB, mapping: ObjectMapping[T]) {
  protected def dataManager: DataManager[T] = mapping.dataManager
  protected def indexer: Indexer[T] = mapping.indexer

  def get(id: Id[T]): IO[Option[T]] = data.get(id)

  def apply(id: Id[T]): IO[T] = data(id)

  def put(id: Id[T], value: T): IO[T] = for {
    _ <- data.put(id, value)
    _ <- indexer.put(id, value)
  } yield {
    value
  }

  def modify(id: Id[T])(f: Option[T] => Option[T]): IO[Option[T]] = for {
    result <- data.modify(id)(f)
    _ <- result.map(indexer.put(id, _)).getOrElse(IO.unit)
  } yield {
    result
  }

  def delete(id: Id[T]): IO[Unit] = for {
    _ <- data.delete(id)
    _ <- indexer.delete(id)
  } yield {
    ()
  }

  def dispose(): IO[Unit] = {
    indexer.dispose()
  }

  protected lazy val data: CollectionData[T] = CollectionData(this)
}

trait Indexer[T] {
  def put(id: Id[T], value: T): IO[T]
  def delete(id: Id[T]): IO[Unit]
  def flush(): IO[Unit]
  def dispose(): IO[Unit]
}

case class LuceneIndexer[T](mapping: ObjectMapping[T], directory: Directory = new ByteBuffersDirectory) extends Indexer[T] {
  private lazy val analyzer = new StandardAnalyzer
  private lazy val config = new IndexWriterConfig(analyzer)
  private lazy val writer = new IndexWriter(directory, config)

  override def put(id: Id[T], value: T): IO[T] = IO {
    val doc = new Document
    doc.add(new StoredField("_id", id.toString))
    mapping.fields.foreach { f =>
      f.features.foreach {
        case lif: LuceneIndexFeature[T] => lif.index(f.name, value, doc)
        case _ => // Ignore
      }
    }
    writer.addDocument(doc)
    value
  }

  override def delete(id: Id[T]): IO[Unit] = IO {
    writer.deleteDocuments(new Term("_id", id.toString))
  }

  override def flush(): IO[Unit] = IO {
    writer.flush()
  }

  override def dispose(): IO[Unit] = IO {
    writer.flush()
    writer.close()
  }

  def string(f: T => String): LuceneIndexFeature[T] = feature(f, (name: String, value: String) =>
    new StringField(name, value, LuceneField.Store.YES))
  def int(f: T => Int): LuceneIndexFeature[T] = feature(f, (name: String, value: Int) => new StoredField(name, value))

  def feature[F](f: T => F, lf: (String, F) => IndexableField): LuceneIndexFeature[T] = new LuceneIndexFeature[T] {
    override def index(name: String, value: T, document: Document): Unit = {
      document.add(lf(name, f(value)))
    }
  }
}

trait LuceneIndexFeature[T] extends FieldFeature {
  def index(name: String, value: T, document: Document): Unit
}

case class CollectionData[T](collection: Collection[T]) {
  protected def db: LightDB = collection.db
  protected def dataManager: DataManager[T] = collection.mapping.dataManager

  def get(id: Id[T]): IO[Option[T]] = db.store.get(id).map(_.map(dataManager.fromArray))

  def apply(id: Id[T]): IO[T] = get(id).map(_.getOrElse(throw new RuntimeException(s"Not found by id: $id")))

  def put(id: Id[T], value: T): IO[T] = db.store.put(id, dataManager.toArray(value)).map(_ => value)

  def modify(id: Id[T])(f: Option[T] => Option[T]): IO[Option[T]] = {
    var result: Option[T] = None
    db.store.modify(id) { bytes =>
      val value = bytes.map(dataManager.fromArray)
      result = f(value)
      result.map(dataManager.toArray)
    }.map(_ => result)
  }

  def delete(id: Id[T]): IO[Unit] = db.store.delete(id)
}

case class Field[T, F](name: String, features: List[FieldFeature])

trait FieldFeature

trait ObjectMapping[T] {
  def fields: List[Field[T, _]]
  def dataManager: DataManager[T]
  def indexer: Indexer[T]

  def collectionName: String

  def field[F](name: String, features: FieldFeature*): Field[T, F] = {
    Field[T, F](name, features.toList)
  }
}

object Test extends LightDB(new HaloStore) with IOApp {
  val people: Collection[Person] = Collection(this, Person)

  case class Person(name: String, age: Int, _id: Id[Person] = Id())

  object Person extends ObjectMapping[Person] {
    implicit val rw: ReaderWriter[Person] = ccRW

    override def collectionName: String = "people"

    lazy val dataManager: DataManager[Person] = new JsonDataManager[Person]
    lazy val indexer: LuceneIndexer[Person] = new LuceneIndexer[Person](this)

    lazy val name: Field[Person, String] = field[String]("name", indexer.string(_.name))
    lazy val age: Field[Person, Int] = field[Int]("age", indexer.int(_.age))

    override lazy val fields: List[Field[Person, _]] = List(name, age)
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val id1 = Id[Person]()
    val id2 = Id[Person]()

    val p1 = Person("John Doe", 21, id1)
    val p2 = Person("Jane Doe", 19, id2)

    for {
      _ <- people.put(id1, p1)
      _ <- people.put(id2, p2)
      g1 <- people.get(id1)
      _ = assert(g1.contains(p1), s"$g1 did not contain $p1")
      g2 <- people.get(id2)
      _ = assert(g2.contains(p2), s"$g2 did not contain $p2")
      // TODO: query from the index
      _ <- dispose()
    } yield {
      ExitCode.Success
    }
  }
}