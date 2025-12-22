package spec

import lightdb.rocksdb.RocksDBStore
import lightdb.store.CollectionManager

/**
 * Test helper: a CollectionManager that creates TraversalStore instances wrapping a RocksDBStore backing.
 */
trait TraversalRocksDBWrappedManager {
  def traversalStoreManager: CollectionManager = new CollectionManager {
    override type S[Doc <: lightdb.doc.Document[Doc], Model <: lightdb.doc.DocumentModel[Doc]] =
      lightdb.traversal.store.TraversalStore[Doc, Model]

    override def create[Doc <: lightdb.doc.Document[Doc], Model <: lightdb.doc.DocumentModel[Doc]](
      db: lightdb.LightDB,
      model: Model,
      name: String,
      path: Option[java.nio.file.Path],
      storeMode: lightdb.store.StoreMode[Doc, Model]
    ): S[Doc, Model] = {
      val backing = RocksDBStore.create(db, model, s"${name}__backing", path, storeMode)
      val indexPath = path.map(p => p.getParent.resolve(s"${name}__tindex"))
      val indexBacking = RocksDBStore.create[lightdb.KeyValue, lightdb.KeyValue.type](
        db,
        lightdb.KeyValue,
        s"${name}__tindex",
        indexPath,
        lightdb.store.StoreMode.All[lightdb.KeyValue, lightdb.KeyValue.type]()
      )
      new lightdb.traversal.store.TraversalStore[Doc, Model](
        name = name,
        path = path,
        model = model,
        backing = backing,
        indexBacking = Some(indexBacking),
        lightDB = db,
        storeManager = this
      )
    }
  }
}


