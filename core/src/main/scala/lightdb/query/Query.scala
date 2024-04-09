package lightdb.query

import cats.effect.IO
import cats.implicits.toTraverseOps
import lightdb.index.SearchContext
import lightdb.{Collection, Document, Id}
import org.apache.lucene.index.StoredFields
import org.apache.lucene.search.{MatchAllDocsQuery, ScoreDoc, SortField, TopFieldDocs, Query => LuceneQuery, Sort => LuceneSort}

case class Query[D <: Document[D]](collection: Collection[D],
                                   filter: Option[Filter[D]] = None,
                                   sort: List[Sort] = Nil,
                                   scoreDocs: Boolean = false,
                                   pageSize: Int = 1_000) {
  def filter(filter: Filter[D]): Query[D] = copy(filter = Some(filter))
  def search()(implicit context: SearchContext[D]): IO[PagedResults[D]] = doSearch(
    context = context,
    offset = 0,
    after = None
  )

  private[query] def doSearch(context: SearchContext[D],
                              offset: Int,
                              after: Option[ScoreDoc]): IO[PagedResults[D]] = IO {
    val q = filter.map(_.asQuery).getOrElse(new MatchAllDocsQuery)
    val sortFields = sort match {
      case Nil => List(SortField.FIELD_SCORE)
      case _ => sort.map(sort2SortField)
    }
    val s = new LuceneSort(sortFields: _*)
    val topFieldDocs: TopFieldDocs = after match {
      case Some(scoreDoc) => context.indexSearcher.searchAfter(scoreDoc, q, pageSize, s, this.scoreDocs)
      case None => context.indexSearcher.search(q, pageSize, s, this.scoreDocs)
    }
    val scoreDocs: List[ScoreDoc] = topFieldDocs.scoreDocs.toList
    val total: Int = topFieldDocs.totalHits.value.toInt
    val storedFields: StoredFields = context.indexSearcher.storedFields()
    val ids: List[Id[D]] = scoreDocs.map(doc => Id[D](storedFields.document(doc.doc).get("_id")))
    PagedResults(
      query = this,
      context = context,
      offset = offset,
      total = total,
      ids = ids,
      lastScoreDoc = scoreDocs.lastOption
    )
  }

  private[query] def sort2SortField(sort: Sort): SortField = sort match {
    case Sort.BestMatch => SortField.FIELD_SCORE
    case Sort.IndexOrder => SortField.FIELD_DOC
    case Sort.ByField(field, reverse) => new SortField(field.fieldName, field.sortType, reverse)
  }
}

case class PagedResults[D <: Document[D]](query: Query[D],
                                          context: SearchContext[D],
                                          offset: Int,
                                          total: Int,
                                          ids: List[Id[D]],
                                          private val lastScoreDoc: Option[ScoreDoc]) {
  lazy val page: Int = offset / query.pageSize
  lazy val pages: Int = math.ceil(total.toDouble / query.pageSize.toDouble).toInt

  def stream: fs2.Stream[IO, D] = fs2.Stream(ids: _*)
    .evalMap(id => query.collection(id))

  def docs: IO[List[D]] = ids.map(id => query.collection(id)).sequence

  def hasNext: Boolean = pages > (page + 1)

  def next(): IO[Option[PagedResults[D]]] = if (hasNext) {
    query.doSearch(
      context = context,
      offset = offset + query.pageSize,
      after = lastScoreDoc
    ).map(Some.apply)
  } else {
    IO.pure(None)
  }
}

trait Filter[D <: Document[D]] {
  protected[lightdb] def asQuery: LuceneQuery
}

object Filter {
  def apply[D <: Document[D]](f: => LuceneQuery): Filter[D] = new Filter[D] {
    override protected[lightdb] def asQuery: LuceneQuery = f
  }
}