package spec

import lightdb.rocksdb.RocksDBStore
import lightdb.store.StoreMode
import lightdb.traversal.store.TraversalManager

/**
 * Test helper: a PrefixScanningStoreManager that creates TraversalStore instances wrapping a RocksDBStore backing.
 *
 * This is used by specs that require PrefixScanningStoreManager (e.g. AbstractTraversalSpec).
 */
trait TraversalRocksDBWrappedPrefixManager {
  def traversalPrefixStoreManager: TraversalManager = new TraversalManager {

    override def create[Doc <: lightdb.doc.Document[Doc], Model <: lightdb.doc.DocumentModel[Doc]](
      db: lightdb.LightDB,
      model: Model,
      name: String,
      path: Option[java.nio.file.Path],
      storeMode: StoreMode[Doc, Model]
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


