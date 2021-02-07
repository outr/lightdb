package lightdb

import lightdb.collection.Collection
import lightdb.data.DataManager
import lightdb.store.ObjectStore

import scala.concurrent.ExecutionContext

class LightDB(val store: ObjectStore) { thisDb =>
  def collection[T](implicit dm: DataManager[T]): Collection[T] = new Collection[T] {
    override protected def db: LightDB = thisDb

    override protected def dataManager: DataManager[T] = dm
  }

  def dispose()(implicit ec: ExecutionContext): Unit = store.dispose()
}