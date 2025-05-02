// TODO: Resurrect!
//package lightdb.store.sharded.manager
//
//import lightdb.Id
//import lightdb.doc.{Document, DocumentModel}
//import lightdb.store.{Collection, Store}
//
//case class HashBasedShardManager[Doc <: Document[Doc], Model <: DocumentModel[Doc]](model: Model,
//                                                                                    shards: Vector[Collection[Doc, Model]]) extends ShardManagerInstance[Doc, Model] {
//  override def shardFor(id: Id[Doc]): Option[Store[Doc, Model]] = {
//    val index = math.abs(id.value.hashCode % shards.length)
//    Some(shards(index))
//  }
//}
//
//object HashBasedShardManager extends ShardManager {
//  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](model: Model, shards: Vector[Collection[Doc, Model]]): ShardManagerInstance[Doc, Model] =
//    HashBasedShardManager(model, shards)
//}