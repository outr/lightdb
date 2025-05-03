package lightdb.store

import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.materialized.MaterializedAggregate
import lightdb.transaction.{CollectionTransaction, Transaction}
import lightdb.{LightDB, Query, SearchResults}
import rapid.Task

import java.nio.file.Path

abstract class Collection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                             path: Option[Path],
                                                                             model: Model,
                                                                             lightDB: LightDB,
                                                                             storeManager: StoreManager) extends Store[Doc, Model](name, path, model, lightDB, storeManager) {
  override type TX <: CollectionTransaction[Doc, Model]
}
