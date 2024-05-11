package lightdb.index

import cats.effect.IO
import lightdb.model.{AbstractCollection, Collection, DocumentAction, DocumentListener, DocumentModel}
import lightdb.query.{PagedResults, Query, SearchContext}
import lightdb.Document

trait IndexSupport[D <: Document[D]] extends DocumentModel[D] {
  private var _collection: Option[AbstractCollection[D]] = None
  protected def collection: AbstractCollection[D] = this match {
    case c: AbstractCollection[_] => c.asInstanceOf[AbstractCollection[D]]
    case _ => _collection.getOrElse(throw new RuntimeException("DocumentModel not initialized with Collection (yet)"))
  }

  def query: Query[D] = Query(this, collection)

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

  def index: Indexer[D]

  def withSearchContext[Return](f: SearchContext[D] => IO[Return]): IO[Return] = index.withSearchContext(f)

  def doSearch(query: Query[D],
               context: SearchContext[D],
               offset: Int,
               after: Option[PagedResults[D]]): IO[PagedResults[D]]

  protected def indexDoc(doc: D, fields: List[IndexedField[_, D]]): IO[Unit]
}