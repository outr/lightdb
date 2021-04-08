package lightdb.index

import cats.effect.IO
import lightdb.field.Field
import lightdb.{Document, Id, IndexFeature, IntIndexed, ObjectMapping, StringIndexed}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index._
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery, ScoreDoc, SearcherFactory, TopDocs}
import org.apache.lucene.store.{ByteBuffersDirectory, Directory, FSDirectory}
import org.apache.lucene.document.{StoredField, StringField, Document => LuceneDoc, Field => LuceneField}
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter
import org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache

import java.nio.file.Path

// TODO: break this out into db-level and collection-level for individual indexes and cleaner design
case class LuceneIndexer(directory: Option[Path] = None) extends Indexer {
  private lazy val indexPath = directory.map(_.resolve("index"))
  private lazy val taxonomyPath = directory.map(_.resolve("taxonomy"))
  private lazy val indexDirectory = indexPath.map(FSDirectory.open).getOrElse(new ByteBuffersDirectory)
  private lazy val taxonomyDirectory = taxonomyPath.map(FSDirectory.open).getOrElse(new ByteBuffersDirectory)

  private lazy val analyzer = new StandardAnalyzer
  private lazy val config = new IndexWriterConfig(analyzer)
  private lazy val indexWriter = new IndexWriter(indexDirectory, config)
  private lazy val taxonomyWriterCache: TaxonomyWriterCache = DirectoryTaxonomyWriter.defaultTaxonomyWriterCache()
  private lazy val taxonomyWriter: DirectoryTaxonomyWriter = new DirectoryTaxonomyWriter(taxonomyDirectory, IndexWriterConfig.OpenMode.CREATE_OR_APPEND, taxonomyWriterCache)
  private lazy val searcherTaxonomyManager = new SearcherTaxonomyManager(indexWriter, new SearcherFactory, taxonomyWriter)

  private def withSearcherAndTaxonomy[Return](f: SearcherAndTaxonomy => Return): IO[Return] = IO {
    searcherTaxonomyManager.maybeRefreshBlocking()
    val instance = searcherTaxonomyManager.acquire()
    try {
      f(instance)
    } finally {
      searcherTaxonomyManager.release(instance)
    }
  }

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
      indexWriter.updateDocument(new Term("_id", value._id.toString), doc)
    }
    value
  }

  override def commit[D <: Document[D]](mapping: ObjectMapping[D]): IO[Unit] = IO {
    indexWriter.commit()
    taxonomyWriter.commit()
    searcherTaxonomyManager.maybeRefreshBlocking()
  }

  override def delete[D <: Document[D]](id: Id[D], mapping: ObjectMapping[D]): IO[Unit] = IO {
    indexWriter.deleteDocuments(new Term("_id", id.toString))
  }

  override def count(): IO[Long] = withSearcherAndTaxonomy { i =>
    i.searcher.getIndexReader.numDocs()
  }

  override def search[D <: Document[D]](limit: Int): IO[PagedResults[D]] = withSearcherAndTaxonomy { i =>
    val topDocs = i.searcher.search(new MatchAllDocsQuery, limit)
    PagedResults[D](0, limit, topDocs, i.searcher)
  }

  override def dispose(): IO[Unit] = IO {
    indexWriter.flush()
    indexWriter.close()
  }

  /*def test(): IO[Unit] = IO {
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
  }*/
}

case class PagedResults[D <: Document[D]](offset: Int, limit: Int, topDocs: TopDocs, searcher: IndexSearcher) {
  lazy val total: Long = topDocs.totalHits.value

  private[index] lazy val scoreDocs = topDocs.scoreDocs

  lazy val documents: List[ResultDoc[D]] = (0 until scoreDocs.length).toList.map(index => ResultDoc(this, index))
}

case class ResultDoc[D <: Document[D]](results: PagedResults[D], index: Int) {
  private lazy val scoreDoc: ScoreDoc = results.scoreDocs(index)
  private lazy val doc = results.searcher.doc(scoreDoc.doc)

  lazy val id: Id[D] = Id(doc.getField("_id").stringValue())
  lazy val value: D = results.

  def apply[F](field: Field[D, F]): F = {
    val lf = doc.getField(field.name)
    val indexFeature = field.features.collectFirst {
      case f: IndexFeature => f
    }.getOrElse(throw new RuntimeException(s"${field.name} has no index associated with it"))
    indexFeature match {
      case StringIndexed => lf.stringValue().asInstanceOf[F]
      case IntIndexed => lf.numericValue().intValue().asInstanceOf[F]
    }
  }
}