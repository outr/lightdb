package lightdb.index

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import lightdb.collection.Collection
import lightdb.field.Field
import lightdb.query.{Filter, Query}
import lightdb.{Document, Id, IndexFeature, IntIndexed, ObjectMapping, StringIndexed}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index._
import org.apache.lucene.search.{BooleanClause, BooleanQuery, CollectionTerminatedException, Collector, CollectorManager, IndexSearcher, LeafCollector, MatchAllDocsQuery, Scorable, ScoreDoc, ScoreMode, SearcherFactory, Sort, TermQuery, TopDocs, TopFieldCollector, Query => LuceneQuery}
import org.apache.lucene.store.{ByteBuffersDirectory, Directory, FSDirectory}
import org.apache.lucene.document.{StoredField, StringField, Document => LuceneDoc, Field => LuceneField}
import org.apache.lucene.facet.{FacetField, FacetResult, FacetsCollector}
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter
import org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache

import java.nio.file.Path
import java.util
import scala.jdk.CollectionConverters._

trait LuceneIndexerSupport {
  def indexer[D <: Document[D]](collection: Collection[D]): Indexer[D] = LuceneIndexer(collection)
}

case class LuceneIndexer[D <: Document[D]](collection: Collection[D]) extends Indexer[D] {
  private lazy val directory: Option[Path] = collection.db.directory.map(_.resolve(collection.collectionName))
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

  def withSearcherAndTaxonomy[Return](f: SearcherAndTaxonomy => Return): IO[Return] = IO {
    withSearcherAndTaxonomyBlocking[Return](f)
  }

  def withSearcherAndTaxonomyBlocking[Return](f: SearcherAndTaxonomy => Return): Return = {
    searcherTaxonomyManager.maybeRefreshBlocking()
    val instance = searcherTaxonomyManager.acquire()
    try {
      f(instance)
    } finally {
      searcherTaxonomyManager.release(instance)
    }
  }

  override def put(value: D): IO[D] = IO {
    val fieldIndexes: List[LuceneDoc => Unit] = collection.mapping.fields.flatMap { f =>
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

  override def commit(): IO[Unit] = IO {
    indexWriter.commit()
    taxonomyWriter.commit()
    searcherTaxonomyManager.maybeRefreshBlocking()
  }

  override def delete(id: Id[D]): IO[Unit] = IO {
    indexWriter.deleteDocuments(new Term("_id", id.toString))
  }

  override def count(): IO[Long] = withSearcherAndTaxonomy { i =>
    i.searcher.getIndexReader.numDocs()
  }

  override def search(query: Query[D]): IO[PagedResults[D]] = withSearcherAndTaxonomy { i =>
    val q: LuceneQuery = query.filters match {
      case Nil => new MatchAllDocsQuery
      case filter :: Nil => filter2Query(filter)
      case filters => {
        val b = new BooleanQuery.Builder
        filters.foreach { f =>
          b.add(filter2Query(f), BooleanClause.Occur.MUST)
        }
        b.build()
      }
    }
    val s: Sort = if (query.sort.nonEmpty) {
      throw new RuntimeException("Unsupported sorting!")
    } else {
      Sort.RELEVANCE
    }
    val maxDoc = math.max(1, i.searcher.getIndexReader.maxDoc())
    val collector = new DocumentCollector[D](s, maxDoc, query)
    i.searcher.search(q, collector)
  }

  private def filter2Query(filter: Filter): LuceneQuery = filter match {
    case Filter.Equals(field, value) => value match {
      case v: String => new TermQuery(new Term(field.name, v))
    }
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

case class PagedResults[D <: Document[D]](query: Query[D], topDocs: TopDocs) {
  lazy val total: Long = topDocs.totalHits.value

  private[index] lazy val scoreDocs = topDocs.scoreDocs

  lazy val documents: List[ResultDoc[D]] = {
    scoreDocs.foreach { sd =>
      scribe.info(s"Score Doc: ${sd.doc}")
    }
    (0 until scoreDocs.length).toList.map(index => ResultDoc(this, index))
  }
}

case class ResultDoc[D <: Document[D]](results: PagedResults[D], index: Int) {
  private lazy val scoreDoc: ScoreDoc = results.scoreDocs(index)
  private lazy val doc = results
    .query
    .collection
    .indexer
    .asInstanceOf[LuceneIndexer[D]]
    .withSearcherAndTaxonomyBlocking(_.searcher.doc(scoreDoc.doc))

  lazy val id: Id[D] = Id(doc.getField("_id").stringValue())

  def get(): IO[D] = results.query.collection(id)

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

class DocumentCollector[D <: Document[D]](sort: Sort,
                                          maxDoc: Int,
                                          query: Query[D]) extends CollectorManager[Collectors, PagedResults[D]] {
  private val totalHitsThreshold: Int = Int.MaxValue
  private val docLimit: Int = math.min(query.offset + query.limit, maxDoc)

  override def newCollector(): Collectors = {
    val topFieldCollector = TopFieldCollector.create(sort, docLimit, totalHitsThreshold)
    val facetsCollector = new FacetsCollector(query.scoreDocs)
    Collectors(topFieldCollector, facetsCollector)
  }

  override def reduce(collectors: util.Collection[Collectors]): PagedResults[D] = {
    val list = collectors.asScala.toList
    val topDocs = list.collect {
      case Collectors(tfc, _) => tfc.topDocs()
    }.toArray
//    val facetsCollector = list.head.facetsCollector
    val topFieldDocs = TopDocs.merge(sort, docLimit, topDocs) match {
      case td if query.offset > 0 => new TopDocs(td.totalHits, td.scoreDocs.slice(query.offset, query.offset + query.limit))
      case td => td
    }
//    var facetResults = Map.empty[FacetField, FacetResult]
    // TODO: see DocumentCollector:46 in lucene4s for facets
    PagedResults[D](query, topFieldDocs)
  }
}

case class Collectors(topFieldCollector: TopFieldCollector, facetsCollector: FacetsCollector) extends Collector {
  override def getLeafCollector(context: LeafReaderContext): LeafCollector = {
    val topFieldLeaf = try {
      Some(topFieldCollector.getLeafCollector(context))
    } catch {
      case _: CollectionTerminatedException => None
    }
    val facetsLeaf = try {
      Some(facetsCollector.getLeafCollector(context))
    } catch {
      case _: CollectionTerminatedException => None
    }
    val leafCollectors = List(topFieldLeaf, facetsLeaf).flatten
    leafCollectors match {
      case Nil => throw new CollectionTerminatedException()
      case leafCollector :: Nil => leafCollector
      case _ => new CollectorsLeafCollector(leafCollectors)
    }
  }

  override def scoreMode(): ScoreMode = {
    val sm1 = topFieldCollector.scoreMode()
    val sm2 = facetsCollector.scoreMode()
    if (sm1 == ScoreMode.COMPLETE || sm2 == ScoreMode.COMPLETE) {
      ScoreMode.COMPLETE
    } else if (sm1 == ScoreMode.TOP_SCORES || sm2 == ScoreMode.TOP_SCORES) {
      ScoreMode.TOP_SCORES
    } else {
      sm1
    }
  }
}

class CollectorsLeafCollector(leafCollectors: List[LeafCollector]) extends LeafCollector {
  override def setScorer(scorer: Scorable): Unit = {
    leafCollectors.foreach { leafCollector =>
      leafCollector.setScorer(scorer)
    }
  }

  override def collect(doc: Int): Unit = {
    leafCollectors.foreach { leafCollector =>
      leafCollector.collect(doc)
    }
  }
}