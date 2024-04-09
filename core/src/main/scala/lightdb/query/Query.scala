package lightdb.query

import cats.effect.IO
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
  def sort(sort: Sort*): Query[D] = copy(sort = this.sort ::: sort.toList)
  def clearSort: Query[D] = copy(sort = Nil)
  def scoreDocs(b: Boolean): Query[D] = copy(scoreDocs = b)
  def pageSize(size: Int): Query[D] = copy(pageSize = size)

  def search()(implicit context: SearchContext[D]): IO[PagedResults[D]] = doSearch(
    context = context,
    offset = 0,
    after = None
  )
  def stream(implicit context: SearchContext[D]): fs2.Stream[IO, D] = {
    val io = search().map { page1 =>
      fs2.Stream.emit(page1) ++ fs2.Stream.unfoldEval(page1) { page =>
        page.next().map(_.map(p => p -> p))
      }
    }
    fs2.Stream.force(io)
      .flatMap(_.stream)
  }

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