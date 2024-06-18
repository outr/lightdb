package lightdb.index

import cats.effect.IO
import lightdb.model.{AbstractCollection, Collection, DocumentAction, DocumentListener, DocumentModel}
import lightdb.query.{Filter, PagedResults, Query, SearchContext}
import lightdb.Document
import lightdb.aggregate.{AggregateFunction, AggregateQuery}
import lightdb.spatial.GeoPoint
import squants.space.Length

trait IndexSupport[D <: Document[D]] extends DocumentModel[D] {
  private var _collection: Option[AbstractCollection[D]] = None

  protected[lightdb] def collection: AbstractCollection[D] = this match {
    case c: AbstractCollection[_] => c.asInstanceOf[AbstractCollection[D]]
    case _ => _collection.getOrElse(throw new RuntimeException("DocumentModel not initialized with Collection (yet)"))
  }

  def query: Query[D, D] = Query(this, collection, doc => IO.pure(doc))

  override protected[lightdb] def initModel(collection: AbstractCollection[D]): Unit = {
    super.initModel(collection)
    _collection = Some(collection)
    collection.commitActions += index.commit()
    collection.postSet += ((action: DocumentAction, doc: D, collection: AbstractCollection[D]) => {
      indexDoc(doc, index.fields).map(_ => Some(doc))
    })
    collection.postDelete += ((action: DocumentAction, doc: D, collection: AbstractCollection[D]) => {
      index.delete(doc._id).map(_ => Some(doc))
    })
  }

  override def reIndex(collection: AbstractCollection[D]): IO[Unit] = for {
    _ <- super.reIndex(collection)
    _ <- index.truncate()
    _ <- collection.stream.evalMap { doc =>
      indexDoc(doc, index.fields)
    }.compile.drain
    _ <- index.commit()
  } yield ()

  def distanceFilter(field: Index[GeoPoint, D], from: GeoPoint, radius: Length): Filter[D] =
    throw new UnsupportedOperationException("Distance filtering is not supported on this indexer")

  def index: Indexer[D]

  def withSearchContext[Return](f: SearchContext[D] => IO[Return]): IO[Return] = index.withSearchContext(f)

  def doSearch[V](query: Query[D, V],
                  context: SearchContext[D],
                  offset: Int,
                  limit: Option[Int],
                  after: Option[PagedResults[D, V]]): IO[PagedResults[D, V]]

  def aggregate(query: AggregateQuery[D])(implicit context: SearchContext[D]): fs2.Stream[IO, Materialized[D]]

  protected def indexDoc(doc: D, fields: List[Index[_, D]]): IO[Unit]
}