package lightdb

import cats.effect.IO
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentModel}
import lightdb.store.StoreManager
import lightdb.util.Initializable

trait LightDB extends Initializable {
  private var _collections = List.empty[Collection[_]]

  def storeManager: StoreManager

  def collections: List[Collection[_]] = _collections

  def collection[D <: Document[D]](model: DocumentModel[D]): Collection[D] = synchronized {
    val c = Collection[D](model, this)
    _collections = c :: _collections
    c
  }

  override protected def initialize(): IO[Unit] = ???

  def dispose(): IO[Unit] = IO.unit
}