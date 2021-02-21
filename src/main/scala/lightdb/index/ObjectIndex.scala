package lightdb.index

import cats.effect.IO
import lightdb.Id
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, StoredField}
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.{ByteBuffersDirectory, Directory, MMapDirectory}

trait ObjectIndex {
  def dispose(): IO[Unit]
}

class LuceneIndex[T](directory: Directory = new ByteBuffersDirectory) extends ObjectIndex {
  private lazy val analyzer = new StandardAnalyzer
  private lazy val config = new IndexWriterConfig(analyzer)
  private lazy val writer = new IndexWriter(directory, config)

  private var _fields = List.empty[Field[T, _]]

  def index(id: Id[T], value: T): IO[T] = IO {
    val doc = new Document
    doc.add(new StoredField("_id", id.toString))
    _fields.foreach(_.index(doc, value))
    writer.addDocument(doc)
    value
  }

  def flush(): IO[Unit] = IO {
    writer.flush()
  }

  def field[F: Indexable](name: String, extractor: T => F): Field[T, F] = {
    val f = Field[T, F](name, implicitly[Indexable[F]], extractor)
    synchronized {
      _fields = f :: _fields
    }
    f
  }

  override def dispose(): IO[Unit] = IO {
    writer.flush()
    writer.close()
  }
}

case class Field[T, F](name: String, indexable: Indexable[F], extractor: T => F) {
  def index(doc: Document, value: T): Unit = indexable.index(doc, name, extractor(value))
}

trait Indexable[F] {
  def index[T](doc: Document, name: String, value: F): Unit
}