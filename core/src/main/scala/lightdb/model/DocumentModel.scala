package lightdb.model

import cats.implicits._
import lightdb.index.IndexedField
import lightdb.{Document, Id, IndexedLinks, Unique}

trait DocumentModel[D <: Document[D]] {
  type Field[F] = IndexedField[F, D]

  private[lightdb] var _indexedLinks = List.empty[IndexedLinks[_, D]]

  def id(value: String = Unique()): Id[D] = Id(value)

  def indexedLinks: List[IndexedLinks[_, D]] = _indexedLinks

  protected[lightdb] def initModel(collection: AbstractCollection[D]): Unit = {
    collection.postSet.add((action: DocumentAction, doc: D, collection: AbstractCollection[D]) => {
      for {
        // Add to IndexedLinks
        _ <- _indexedLinks.map(_.add(doc)).sequence
      } yield Some(doc)
    })
    collection.postDelete.add((action: DocumentAction, doc: D, collection: AbstractCollection[D]) => {
      for {
        // Remove from IndexedLinks
        _ <- _indexedLinks.map(_.remove(doc)).sequence
      } yield Some(doc)
    })
  }
}
