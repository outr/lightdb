package lightdb.traversal.store

import lightdb.doc.{Document, DocumentModel}
import lightdb.store.CollectionManager
import lightdb.store.prefix.PrefixScanningStoreManager

/**
 * A CollectionManager whose created stores are traversal-backed (`TraversalStore`) and therefore have a concrete
 * transaction type (`TraversalTransaction`).
 *
 * This is primarily useful in tests/specs to avoid `asInstanceOf[TraversalTransaction[...]]` casts.
 */
trait TraversalManager extends CollectionManager with PrefixScanningStoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = TraversalStore[Doc, Model]
}


