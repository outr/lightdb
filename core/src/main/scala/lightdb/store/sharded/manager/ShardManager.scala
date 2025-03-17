package lightdb.store.sharded.manager

import lightdb.doc.{Document, DocumentModel}
import lightdb.store.Store

trait ShardManager {
  def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](model: Model, shards: Vector[Store[Doc, Model]]): ShardManagerInstance[Doc, Model]
}