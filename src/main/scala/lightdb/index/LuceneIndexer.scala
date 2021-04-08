package lightdb.index

import cats.effect.IO
import lightdb.{Document, Id, ObjectMapping}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index._
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery}
import org.apache.lucene.store.{ByteBuffersDirectory, Directory}
import org.apache.lucene.document.{StoredField, StringField, Document => LuceneDoc, Field => LuceneField}

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
