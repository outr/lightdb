package lightdb.rocksdb

import org.rocksdb.ColumnFamilyHandle

case class RocksDBSharedStoreInstance(store: RocksDBSharedStore, handle: String) {
  def existingHandle: Option[ColumnFamilyHandle] = store.existingHandlesMap.get(handle)
}