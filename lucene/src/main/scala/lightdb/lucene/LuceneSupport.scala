package lightdb.lucene

import cats.effect.IO
import fabric.define.DefType
import lightdb._
import lightdb.index.{IndexSupport, Index}
import lightdb.model.AbstractCollection
import lightdb.query.{Filter, PageContext, PagedResults, Query, SearchContext, Sort, SortDirection}
import lightdb.spatial.GeoPoint
import org.apache.lucene.document.{LatLonDocValuesField, LatLonPoint}
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery, ScoreDoc, SearcherFactory, SearcherManager, SortField, SortedNumericSortField, TopFieldDocs, Query => LuceneQuery, Sort => LuceneSort}
import org.apache.lucene.index.StoredFields
import squants.space.Length

trait LuceneSupport[D <: Document[D]] extends IndexSupport[D] {
  override lazy val index: LuceneIndexer[D] = LuceneIndexer(this)

  val _id: Index[Id[D], D] = index("_id", doc => List(doc._id), store = true)

  protected[lucene] def indexSearcher(context: SearchContext[D]): IndexSearcher = index.contextMapping.get(context)

  private def sort2SortField(sort: Sort): SortField = sort match {
    case Sort.BestMatch => SortField.FIELD_SCORE
    case Sort.IndexOrder => SortField.FIELD_DOC
    case Sort.ByField(field, dir) =>
      val f = field.asInstanceOf[LuceneIndex[_, D]]
      f.rw.definition match {
        case DefType.Int => new SortedNumericSortField(field.fieldName, f.sortType, dir == SortDirection.Descending)
        case DefType.Str => new SortField(field.fieldName, f.sortType, dir == SortDirection.Descending)
        case d => throw new RuntimeException(s"Unsupported sort definition: $d")
      }
    case Sort.ByDistance(field, from) =>
      val f = field.asInstanceOf[LuceneIndex[_, D]]
      LatLonDocValuesField.newDistanceSort(f.fieldSortName, from.latitude, from.longitude)
  }

  override def doSearch[V](query: Query[D, V],
                           context: SearchContext[D],
                           offset: Int,
                           limit: Option[Int],
                           after: Option[PagedResults[D, V]]): IO[PagedResults[D, V]] = IO.blocking {
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
    val scoreDocs: List[ScoreDoc] = topFieldDocs.scoreDocs.toList.take(limit.getOrElse(Int.MaxValue) - offset)
    val total: Int = topFieldDocs.totalHits.value.toInt
    val storedFields: StoredFields = indexSearcher.storedFields()
    val idsAndScores = scoreDocs.map(doc => Id[D](storedFields.document(doc.doc).get("_id")) -> doc.score.toDouble)
    val indexContext = LucenePageContext(
      context = context,
      lastScoreDoc = scoreDocs.lastOption
    )
    PagedResults(
      query = query,
      context = indexContext,
      offset = offset,
      total = total,
      idsAndScores = idsAndScores
    )
  }

  override protected def indexDoc(doc: D, fields: List[Index[_, D]]): IO[Unit] = for {
    fields <- IO.blocking(fields.flatMap { field =>
      field.asInstanceOf[LuceneIndex[_, D]].createFields(doc)
    })
    _ = index.addDoc(doc._id, fields)
  } yield ()

  override protected[lightdb] def initModel(collection: AbstractCollection[D]): Unit = {
    super.initModel(collection)
    collection.truncateActions.add(index.truncate())
  }

  override def distanceFilter(field: Index[GeoPoint, D], from: GeoPoint, distance: Length): Filter[D] =
    LuceneFilter(() => LatLonPoint.newDistanceQuery(field.fieldName, from.latitude, from.longitude, distance.toMeters))
}