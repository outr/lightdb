package lightdb.index.lucene

import cats.effect.IO
import lightdb.{Document, Id}
import lightdb.collection.Collection
import lightdb.index.{Indexer, SearchResult}
import lightdb.query.{Filter, Query}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{DirectoryReader, IndexReader, IndexWriter, IndexWriterConfig, StoredFields}
import org.apache.lucene.store.{ByteBuffersDirectory, FSDirectory, MMapDirectory}
import org.apache.lucene.document.{Field, IntField, TextField, Document => LuceneDocument}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery, ScoreDoc, SearcherFactory, SearcherManager}

import java.nio.file.{Files, Path}
import java.util.Comparator
import scala.collection.immutable.ArraySeq

trait LuceneIndexerSupport {
  protected def autoCommit: Boolean = false

  def indexer[D <: Document[D]](collection: Collection[D]): Indexer[D] = LuceneIndexer(collection, autoCommit)
}

case class LuceneIndexer[D <: Document[D]](collection: Collection[D],
                                           autoCommit: Boolean = false,
                                           analyzer: Analyzer = new StandardAnalyzer) extends Indexer[D] { i =>
  private var disposed = false
  private lazy val path: Option[Path] = collection.db.directory.map(_.resolve(collection.collectionName))
  private lazy val directory = path
    .map(p => FSDirectory.open(p))
    .getOrElse(new ByteBuffersDirectory())

  private lazy val config = new IndexWriterConfig(analyzer)
  private lazy val indexWriter = new IndexWriter(directory, config)

  private var _indexSearcher: IndexSearcher = _
  private lazy val searcherManager = new SearcherManager(indexWriter, new SearcherFactory)
  private def indexSearcher: IndexSearcher = synchronized {
    if (_indexSearcher == null) {
      _indexSearcher = searcherManager.acquire()
    }
    _indexSearcher
  }

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
    indexWriter.flush()
    indexWriter.commit()
    i.synchronized {
      if (_indexSearcher != null) {
        searcherManager.release(_indexSearcher)
      }
      _indexSearcher = null
    }
  }

  override def count(): IO[Long] = IO {
    indexSearcher.count(new MatchAllDocsQuery)
  }

  override def search(query: Query[D]): fs2.Stream[IO, SearchResult[D]] = {
    val indexSearch = this.indexSearcher
    val parser = new QueryParser("_id", analyzer)
    val filters = query.filters.map {
      case Filter.Equals(field, value) => s"${field.name}:$value"
      case f => throw new UnsupportedOperationException(s"Unsupported filter: $f")
    }
    // TODO: Support filtering better
    val filterString = filters match {
      case f :: Nil => f
      case list => list.mkString("(", " AND ", ")")
    }
    val q = parser.parse(filterString)
    val topDocs = indexSearcher.search(q, query.batchSize)
    val hits = topDocs.scoreDocs
    val total = topDocs.totalHits.value
    val storedFields = indexSearcher.storedFields()
    fs2.Stream[IO, ScoreDoc](ArraySeq.unsafeWrapArray(hits): _*)
      .map { sd =>
        LuceneSearchResult(sd, total, query, storedFields)
      }
  }

  override def truncate(): IO[Unit] = for {
    _ <- close()
    _ <- IO {
      path.foreach { p =>
        Files.walk(p)
          .sorted(Comparator.reverseOrder())
          .map(_.toFile)
          .forEach(f => f.delete())
      }
    }
  } yield ()

  def close(): IO[Unit] = IO {
    indexWriter.flush()
    indexWriter.commit()
    indexWriter.close()
    if (_indexSearcher != null) {
      searcherManager.release(_indexSearcher)
    }
    searcherManager.close()
  }

  override def dispose(): IO[Unit] = close().map { _ =>
    disposed = true
  }

  private case class LuceneSearchResult(scoreDoc: ScoreDoc,
                                        total: Long,
                                        query: Query[D],
                                        storedFields: StoredFields) extends SearchResult[D] {
    private lazy val document = storedFields.document(scoreDoc.doc)
    private lazy val doc = collection(id)

    lazy val id: Id[D] = Id[D](document.get("_id"))

    override def get(): IO[D] = doc
    override def apply[F](field: _root_.lightdb.field.Field[D, F]): F = ???
  }
}