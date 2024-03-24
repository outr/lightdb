/*
    Analyzer analyzer = new StandardAnalyzer();

    Path indexPath = Files.createTempDirectory("tempIndex");
    Directory directory = FSDirectory.open(indexPath);
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    IndexWriter iwriter = new IndexWriter(directory, config);
    Document doc = new Document();
    String text = "This is the text to be indexed.";
    doc.add(new Field("fieldname", text, TextField.TYPE_STORED));
    iwriter.addDocument(doc);
    iwriter.close();

    // Now search the index:
    DirectoryReader ireader = DirectoryReader.open(directory);
    IndexSearcher isearcher = new IndexSearcher(ireader);
    // Parse a simple query that searches for "text":
    QueryParser parser = new QueryParser("fieldname", analyzer);
    Query query = parser.parse("text");
    ScoreDoc[] hits = isearcher.search(query, 10).scoreDocs;
    assertEquals(1, hits.length);
    // Iterate through the results:
    StoredFields storedFields = isearcher.storedFields();
    for (int i = 0; i < hits.length; i++) {
      Document hitDoc = storedFields.document(hits[i].doc);
      assertEquals("This is the text to be indexed.", hitDoc.get("fieldname"));
    }
    ireader.close();
    directory.close();
    IOUtils.rm(indexPath);
 */

package lightdb.index.lucene

import cats.effect.IO
import lightdb.{Document, Id, field}
import lightdb.collection.Collection
import lightdb.index.{Indexer, SearchResult}
import lightdb.query.{Filter, Query}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.document.{Field, IntField, TextField, Document => LuceneDocument}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.IndexSearcher

trait LuceneIndexerSupport {
  protected def autoCommit: Boolean = false

  def indexer[D <: Document[D]](collection: Collection[D]): Indexer[D] = LuceneIndexer(collection, autoCommit)
}

case class LuceneIndexer[D <: Document[D]](collection: Collection[D], autoCommit: Boolean = false) extends Indexer[D] {
  private lazy val analyzer = new StandardAnalyzer
  private lazy val directory = FSDirectory.open(collection.db.directory.map(_.resolve(collection.collectionName)).get)
  private lazy val config = new IndexWriterConfig(analyzer)
  private lazy val indexWriter = new IndexWriter(directory, config)

  private lazy val indexReader = DirectoryReader.open(directory)
  private lazy val indexSearcher = new IndexSearcher(indexReader)

  override def put(value: D): IO[D] = IO {
    val document = new LuceneDocument
    collection.mapping.fields.foreach { field =>
      field.getter(value) match {
        case id: Id[_] => document.add(new Field(field.name, id.value, TextField.TYPE_STORED))
        case s: String => document.add(new Field(field.name, s, TextField.TYPE_STORED))
        case i: Int => document.add(new IntField(field.name, i, Field.Store.YES))
        case value => throw new RuntimeException(s"Unsupported value: $value (${value.getClass})")
      }
    }
    if (document.iterator().hasNext) {
      indexWriter.addDocument(document)
    }
    value
  }

  override def delete(id: Id[D]): IO[Unit] = IO.unit

  override def commit(): IO[Unit] = IO {
    scribe.info(s"COMMIT! ${collection.collectionName}")
    indexWriter.flush()
    indexWriter.close()
  }

  override def count(): IO[Long] = IO {
    scribe.info(s"COUNT! ${collection.collectionName}")
    indexReader.getDocCount("_id")
  }

  override def search(query: Query[D]): fs2.Stream[IO, SearchResult[D]] = {
    val parser = new QueryParser("_id", analyzer)
    val filters = query.filters.map {
      case Filter.Equals(field, value) => s"${field.name}:\"$value\""
      case f => throw new UnsupportedOperationException(s"Unsupported filter: $f")
    }
    val filterString = filters match {
      case f :: Nil => f
      case list => list.mkString("(", " AND ", ")")
    }
    val q = parser.parse(filterString)
    val hits = indexSearcher.search(q, query.batchSize).scoreDocs
    val storedFields = indexSearcher.storedFields()
    val results = hits.toList.map { sd =>
      val document = storedFields.document(sd.doc)
      val id = Id[D](document.get("_id"))
      val d = collection(id)
      new SearchResult[D] {
        override def query: Query[D] = query
        override def total: Long = ???
        override def id: Id[D] = id
        override def get(): IO[D] = d
        override def apply[F](field: _root_.lightdb.field.Field[D, F]): F = ???
      }
    }
    fs2.Stream[IO, SearchResult[D]](results: _*)
  }

  override def truncate(): IO[Unit] = ???

  override def dispose(): IO[Unit] = ???
}