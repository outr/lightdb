package lightdb.index

import cats.effect.IO
import lightdb.{Document, Id, IndexFeature, IntIndexed, ObjectMapping, StringIndexed}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index._
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery}
import org.apache.lucene.store.{ByteBuffersDirectory, Directory}
import org.apache.lucene.document.{StoredField, StringField, Document => LuceneDoc, Field => LuceneField}

case class LuceneIndexer(directory: Directory = new ByteBuffersDirectory) extends Indexer {
  private lazy val analyzer = new StandardAnalyzer
  private lazy val config = new IndexWriterConfig(analyzer)
  private lazy val writer = new IndexWriter(directory, config)
  private lazy val reader: DirectoryReader = DirectoryReader.open(writer)
  private lazy val searcher: IndexSearcher = new IndexSearcher(reader)

  override def put[D <: Document[D]](value: D, mapping: ObjectMapping[D]): IO[D] = IO {
    val fieldIndexes: List[LuceneDoc => Unit] = mapping.fields.flatMap { f =>
      f.features.collect {
        case indexFeature: IndexFeature => indexFeature match {
          case StringIndexed => (d: LuceneDoc) => {
            d.add(new StringField(f.name, f.getter(value).asInstanceOf[String], LuceneField.Store.YES))
          }
          case IntIndexed => (d: LuceneDoc) => {
            d.add(new StoredField(f.name, f.getter(value).asInstanceOf[Int]))
          }
        }
      }
    }
    if (fieldIndexes.nonEmpty) {
      val doc = new LuceneDoc
      doc.add(new StringField("_id", value._id.toString, LuceneField.Store.YES))
      fieldIndexes.foreach { fi =>
        fi(doc)
      }
      writer.updateDocument(new Term("_id", value._id.toString), doc)
    }
    value
  }

  override def commit[D <: Document[D]](mapping: ObjectMapping[D]): IO[Unit] = IO {
    writer.commit()
  }

  override def delete[D <: Document[D]](id: Id[D], mapping: ObjectMapping[D]): IO[Unit] = IO {
    writer.deleteDocuments(new Term("_id", id.toString))
  }

  override def count(): IO[Long] = IO {
    reader.numDocs()
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
}
