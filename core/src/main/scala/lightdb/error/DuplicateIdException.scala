package lightdb.error

import lightdb.id.Id

/**
 * Thrown by `Transaction.insert` when a document with the same `_id` already exists in the store.
 *
 * Use `Transaction.upsert` instead when create-or-replace semantics are intended.
 */
case class DuplicateIdException(storeName: String,
                                id: Id[?])
  extends RuntimeException(s"Duplicate _id '${id.value}' in store '$storeName' — use upsert to replace, or delete first")
