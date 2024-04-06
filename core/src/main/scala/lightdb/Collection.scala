package lightdb

import fabric.rw.RW

abstract class Collection[D <: Document[D]](implicit val rw: RW[D]) {
  protected lazy val defaultCollectionName: String = getClass.getSimpleName.replace("$", "")
  protected lazy val store: Store = db.createStore(collectionName)
  protected lazy val indexStore: Store = db.createStore(s"$collectionName.indexes")

  protected def db: LightDB
  protected def collectionName: String = defaultCollectionName

  // TODO: Triggers
}