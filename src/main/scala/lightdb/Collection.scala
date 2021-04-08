package lightdb

import cats.effect.{ExitCode, IO, IOApp}
import fabric.rw._
import lightdb.data.{DataManager, JsonDataManager}
import lightdb.store.HaloStore
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document => LuceneDoc, StoredField, StringField, Field => LuceneField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig, IndexableField, Term}
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery, Query}
import org.apache.lucene.store.{ByteBuffersDirectory, Directory, FSDirectory}

import java.nio.file.Paths

trait Document[D <: Document[D]] {
  def _id: Id[D]
}

case class Collection[D <: Document[D]](db: LightDB, mapping: ObjectMapping[D]) {
  protected def dataManager: DataManager[D] = mapping.dataManager
  protected def indexer: Indexer[D] = mapping.indexer

  def get(id: Id[D]): IO[Option[D]] = data.get(id)

  def apply(id: Id[D]): IO[D] = data(id)

  def put(value: D): IO[D] = for {
    _ <- data.put(value._id, value)
    _ <- indexer.put(value)
  } yield {
    value
  }

  def modify(id: Id[D])(f: Option[D] => Option[D]): IO[Option[D]] = for {
    result <- data.modify(id)(f)
    _ <- result.map(indexer.put).getOrElse(IO.unit)
  } yield {
    result
  }

  def delete(id: Id[D]): IO[Unit] = for {
    _ <- data.delete(id)
    _ <- indexer.delete(id)
  } yield {
    ()
  }

  def dispose(): IO[Unit] = {
    indexer.dispose()
  }

  protected lazy val data: CollectionData[D] = CollectionData(this)
}

trait Indexer[D <: Document[D]] {
  def put(value: D): IO[D]
  def delete(id: Id[D]): IO[Unit]
  def flush(): IO[Unit]
  def dispose(): IO[Unit]
}

case class LuceneIndexer[D <: Document[D]](mapping: ObjectMapping[D], directory: Directory = new ByteBuffersDirectory) extends Indexer[D] {
  private lazy val analyzer = new StandardAnalyzer
  private lazy val config = new IndexWriterConfig(analyzer)
  private lazy val writer = new IndexWriter(directory, config)
  private lazy val reader: DirectoryReader = DirectoryReader.open(writer)
  private lazy val searcher: IndexSearcher = new IndexSearcher(reader)

  override def put(value: D): IO[D] = IO {
    val doc = new LuceneDoc
    doc.add(new StringField("_id", value._id.toString, LuceneField.Store.YES))
    mapping.fields.foreach { f =>
      f.features.foreach {
        case lif: LuceneIndexFeature[D] => lif.index(f.name, value, doc)
        case _ => // Ignore
      }
    }
    writer.updateDocument(new Term("_id", value._id.toString), doc)
    writer.commit()
    value
  }

  override def delete(id: Id[D]): IO[Unit] = IO {
    writer.deleteDocuments(new Term("_id", id.toString))
  }

  override def flush(): IO[Unit] = IO {
    writer.flush()
  }

  override def dispose(): IO[Unit] = IO {
    writer.flush()
    writer.close()
  }

  def test(): IO[Unit] = IO {
    val topDocs = searcher.search(new MatchAllDocsQuery, 1000)
    scribe.info(s"Total Hits: ${topDocs.totalHits.value}")
    val scoreDocs = topDocs.scoreDocs.toVector
    scoreDocs.foreach { sd =>
      val docId = sd.doc
      val doc = searcher.doc(docId)
      val id = doc.getField("_id").stringValue()
      val name = doc.getField("name").stringValue()
      val age = doc.getField("age").numericValue().intValue()
      scribe.info(s"ID: $id, Name: $name, Age: $age")
    }
  }

  def string(f: D => String): LuceneIndexFeature[D] = feature(f, (name: String, value: String) =>
    new StringField(name, value, LuceneField.Store.YES))
  def int(f: D => Int): LuceneIndexFeature[D] = feature(f, (name: String, value: Int) => new StoredField(name, value))

  def feature[F](f: D => F, lf: (String, F) => IndexableField): LuceneIndexFeature[D] = new LuceneIndexFeature[D] {
    override def index(name: String, value: D, document: LuceneDoc): Unit = {
      document.add(lf(name, f(value)))
    }
  }
}

trait LuceneIndexFeature[T] extends FieldFeature {
  def index(name: String, value: T, document: LuceneDoc): Unit
}

case class CollectionData[D <: Document[D]](collection: Collection[D]) {
  protected def db: LightDB = collection.db
  protected def dataManager: DataManager[D] = collection.mapping.dataManager

  def get(id: Id[D]): IO[Option[D]] = db.store.get(id).map(_.map(dataManager.fromArray))

  def apply(id: Id[D]): IO[D] = get(id).map(_.getOrElse(throw new RuntimeException(s"Not found by id: $id")))

  def put(id: Id[D], value: D): IO[D] = db.store.put(id, dataManager.toArray(value)).map(_ => value)

  def modify(id: Id[D])(f: Option[D] => Option[D]): IO[Option[D]] = {
    var result: Option[D] = None
    db.store.modify(id) { bytes =>
      val value = bytes.map(dataManager.fromArray)
      result = f(value)
      result.map(dataManager.toArray)
    }.map(_ => result)
  }

  def delete(id: Id[D]): IO[Unit] = db.store.delete(id)
}

case class Field[T, F](name: String, features: List[FieldFeature])

trait FieldFeature

trait ObjectMapping[D <: Document[D]] {
  def fields: List[Field[D, _]]
  def dataManager: DataManager[D]
  def indexer: Indexer[D]

  def collectionName: String

  def field[F](name: String, features: FieldFeature*): Field[D, F] = {
    Field[D, F](name, features.toList)
  }
}

object Test extends LightDB(new HaloStore) with IOApp {
  val people: Collection[Person] = Collection(this, Person)

  case class Person(name: String, age: Int, _id: Id[Person] = Id()) extends Document[Person]

  object Person extends ObjectMapping[Person] {
    implicit val rw: ReaderWriter[Person] = ccRW

    override def collectionName: String = "people"

    lazy val dataManager: DataManager[Person] = new JsonDataManager[Person]
    lazy val indexer: LuceneIndexer[Person] = new LuceneIndexer[Person](this, FSDirectory.open(Paths.get("db/index")))

    lazy val name: Field[Person, String] = field[String]("name", indexer.string(_.name))
    lazy val age: Field[Person, Int] = field[Int]("age", indexer.int(_.age))

    override lazy val fields: List[Field[Person, _]] = List(name, age)
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val id1 = Id[Person]("john")
    val id2 = Id[Person]("jane")

    val p1 = Person("John Doe", 21, id1)
    val p2 = Person("Jane Doe", 19, id2)

    for {
      _ <- people.put(p1)
      _ <- people.put(p2)
      g1 <- people.get(id1)
      _ = assert(g1.contains(p1), s"$g1 did not contain $p1")
      g2 <- people.get(id2)
      _ = assert(g2.contains(p2), s"$g2 did not contain $p2")
      // TODO: query from the index
      _ <- people.mapping.indexer.asInstanceOf[LuceneIndexer[Person]].test()
      _ <- dispose()
    } yield {
      ExitCode.Success
    }
  }
}