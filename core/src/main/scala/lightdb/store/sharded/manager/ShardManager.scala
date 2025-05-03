// TODO: Resurrect!
//package lightdb.store.sharded.manager
//
//import lightdb.doc.{Document, DocumentModel}
//import lightdb.store.Collection
//
//trait ShardManager {
//  def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](model: Model, shards: Vector[Collection[Doc, Model]]): ShardManagerInstance[Doc, Model]
//}