package lightdb.lucene

import cats.effect.IO
import lightdb._
import lightdb.index.{IndexSupport, IndexedField, Indexer}
import lightdb.lucene.index._
import lightdb.query.{Filter, PageContext, PagedResults, Query, SearchContext, Sort}
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery, ScoreDoc, SearcherFactory, SearcherManager, SortField, TopFieldDocs, Query => LuceneQuery, Sort => LuceneSort}
import org.apache.lucene.{document => ld}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, StoredFields, Term}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.store.{ByteBuffersDirectory, FSDirectory}
import org.apache.lucene.document.{Document => LuceneDocument, Field => LuceneField}

import java.nio.file.{Files, Path}
import java.util.concurrent.ConcurrentHashMap

trait LuceneSupport[D <: Document[D]] extends IndexSupport[D] {
  override lazy val index: LuceneIndexer[D] = LuceneIndexer(this)

  val _id: StringField[D] = index("_id").string(_._id.value, store = true)

  protected[lucene] def indexSearcher(context: SearchContext[D]): IndexSearcher = index.contextMapping.get(context)

  private def sort2SortField(sort: Sort): SortField = sort match {
    case Sort.BestMatch => SortField.FIELD_SCORE
    case Sort.IndexOrder => SortField.FIELD_DOC
    case Sort.ByField(field, reverse) => new SortField(field.fieldName, field.asInstanceOf[LuceneIndexedField[_, D]].sortType, reverse)
  }

  override def doSearch(query: Query[D],
                        context: SearchContext[D],
                        offset: Int,
                        after: Option[PagedResults[D]]): IO[PagedResults[D]] = IO {
    val q = query.filter.map(_.asInstanceOf[LuceneFilter[D]].asQuery()).getOrElse(new MatchAllDocsQuery)
    val sortFields = query.sort match {
      case Nil => List(SortField.FIELD_SCORE)
      case _ => query.sort.map(sort2SortField)
    }
    val s = new LuceneSort(sortFields: _*)
    val indexSearcher = query.indexSupport.asInstanceOf[LuceneSupport[D]].indexSearcher(context)
    val topFieldDocs: TopFieldDocs = after match {
      case Some(afterPage) =>
        val afterDoc = afterPage.context.asInstanceOf[LucenePageContext[D]].lastScoreDoc.get
        indexSearcher.searchAfter(afterDoc, q, query.pageSize, s, query.scoreDocs)
      case None => indexSearcher.search(q, query.pageSize, s, query.scoreDocs)
    }
    val scoreDocs: List[ScoreDoc] = topFieldDocs.scoreDocs.toList
    val total: Int = topFieldDocs.totalHits.value.toInt
    val storedFields: StoredFields = indexSearcher.storedFields()
    val ids: List[Id[D]] = scoreDocs.map(doc => Id[D](storedFields.document(doc.doc).get("_id")))
    val indexContext = LucenePageContext(
      context = context,
      lastScoreDoc = scoreDocs.lastOption
    )
    PagedResults(
      query = query,
      context = indexContext,
      offset = offset,
      total = total,
      ids = ids
    )
  }

  override protected def indexDoc(doc: D, fields: List[IndexedField[_, D]]): IO[Unit] = for {
    fields <- IO(fields.flatMap { field =>
      field.asInstanceOf[LuceneIndexedField[_, D]].createFields(doc)
    })
    _ = index.addDoc(doc._id, fields)
  } yield ()
}