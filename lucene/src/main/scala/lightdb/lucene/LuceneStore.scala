package lightdb.lucene

import fabric._
import fabric.define.DefType
import fabric.io.JsonFormatter
import fabric.rw.Asable
import lightdb._
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field._
import lightdb.field.{Field, IndexingState}
import lightdb.filter.Filter
import lightdb.lucene.index.Index
import lightdb.materialized.MaterializedAggregate
import lightdb.spatial.Geo
import lightdb.store._
import lightdb.transaction.Transaction
import lightdb.util.Aggregator
import org.apache.lucene.document.{DoubleDocValuesField, DoubleField, IntField, LatLonDocValuesField, LatLonPoint, LatLonShape, LongField, NumericDocValuesField, SortedDocValuesField, StoredField, StringField, TextField, Document => LuceneDocument, Field => LuceneField}
import org.apache.lucene.facet.{FacetsConfig, FacetField => LuceneFacetField}
import org.apache.lucene.geo.{Line, Polygon}
import org.apache.lucene.index.{DirectoryReader, SegmentReader, Term}
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery}
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.{BytesRef, Version}
import rapid._

import java.nio.file.{Files, Path}
import scala.language.implicitConversions

class LuceneStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                     path: Option[Path],
                                                                     model: Model,
                                                                     val storeMode: StoreMode[Doc, Model],
                                                                     lightDB: LightDB,
                                                                     storeManager: StoreManager) extends Collection[Doc, Model](name, path, model, lightDB, storeManager) {
  override type TX = LuceneTransaction[Doc, Model]

  IndexSearcher.setMaxClauseCount(10_000_000)

  lazy val index = Index(path)
  lazy val facetsConfig: FacetsConfig = {
    val c = new FacetsConfig
    fields.foreach {
      case ff: FacetField[_] =>
        if (ff.hierarchical) c.setHierarchical(ff.name, ff.hierarchical)
        if (ff.multiValued) c.setMultiValued(ff.name, ff.multiValued)
        if (ff.requireDimCount) c.setRequireDimCount(ff.name, ff.requireDimCount)
      case _ => // Ignore
    }
    c
  }
  private[lucene] lazy val hasFacets: Boolean = fields.exists(_.isInstanceOf[FacetField[_]])

  override protected def initialize(): Task[Unit] = super.initialize().next(Task {
    this.path.foreach { path =>
      if (Files.exists(path)) {
        val directory = FSDirectory.open(path)
        val reader = DirectoryReader.open(directory)
        reader.leaves().forEach { leaf =>
          val dataVersion = leaf.reader().asInstanceOf[SegmentReader].getSegmentInfo.info.getVersion
          val latest = Version.LATEST
          if (latest != dataVersion) {
            // TODO: Support re-indexing
            scribe.warn(s"Data Version: $dataVersion, Latest Version: $latest")
          }
        }
      }
    }
  })

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]]): Task[TX] = Task {
    LuceneTransaction(this, LuceneState[Doc](index, hasFacets), parent)
  }

  override def optimize(): Task[Unit] = Task {
    val s = index.createIndexSearcher()
    val currentSegments = try {
      s.getIndexReader.leaves().size()
    } finally {
      index.releaseIndexSearch(s)
    }
    scribe.info(s"Optimizing Lucene Index for $name. Current segment count: $currentSegments")
    index.indexWriter.forceMerge(1)
  }

  override protected def doDispose(): Task[Unit] = super.doDispose().next(Task {
    index.dispose()
  })
}

object LuceneStore extends CollectionManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = LuceneStore[Doc, Model]

  private val regexChars = ".?+*|{}[]()\"\\#".toSet
  def escapeRegexLiteral(s: String): String = s.flatMap(c => if (regexChars.contains(c)) s"\\$c" else c.toString)

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] =
    new LuceneStore[Doc, Model](name, path, model, storeMode, db, this)
}