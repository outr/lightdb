package lightdb.sql

import lightdb.doc.{Document, DocumentModel}
import lightdb.store.CollectionManager

/**
 * A CollectionManager whose stores are SQL-based (i.e., extend [[SQLStore]]).
 *
 * This is primarily useful for improving type inference so that `store.transaction { tx => ... }`
 * can expose `SQLStoreTransaction` without casts.
 */
trait SQLCollectionManager extends CollectionManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] <: SQLStore[Doc, Model]
}


