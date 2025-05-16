// TODO: Resurrect!
//package lightdb.store.sharded.manager
//
//import lightdb.Id
//import lightdb.doc.{Document, DocumentModel}
//import lightdb.field.Field.UniqueIndex
//import lightdb.store.{Collection, Store}
//import lightdb.transaction.Transaction
//import rapid.Task
//
//trait ShardManagerInstance[Doc <: Document[Doc], Model <: DocumentModel[Doc]] {
//  protected def model: Model
//
//  def shards: Vector[Collection[Doc, Model]]
//
//  def shardFor(id: Id[Doc]): Option[Store[Doc, Model]]
//
//  def findDocShard(id: Id[Doc])(transaction: Transaction[Doc]): Task[Option[Store[Doc, Model]]] = firstMatch { store =>
//    store.get(id).map(_.map(_ => store))
//  }
//
//  protected def shardFor(doc: Doc): Store[Doc, Model] = shardFor(doc._id)
//    .getOrElse(throw new RuntimeException(s"No shard found for ${doc._id}"))
//
//  protected def firstMatch[Return](f: Store[Doc, Model] => Task[Option[Return]]): Task[Option[Return]] = {
//    def recurse(shards: Vector[Store[Doc, Model]]): Task[Option[Return]] = if (shards.isEmpty) {
//      Task.pure(None)
//    } else {
//      val shard = shards.head
//      f(shard).flatMap {
//        case Some(r) => Task.pure(Some(r))
//        case None => recurse(shards.tail)
//      }
//    }
//
//    recurse(shards)
//  }
//
//  def insert(doc: Doc)(transaction: Transaction[Doc]): Task[Doc] = shardFor(doc)
//    .insert(doc)
//
//  def upsert(doc: Doc)(transaction: Transaction[Doc]): Task[Doc] = shardFor(doc)
//    .upsert(doc)
//
//  def delete[V](field: UniqueIndex[Doc, V], value: V)(transaction: Transaction[Doc]): Task[Option[Store[Doc, Model]]] = {
//    val deleteFirst = () => shards.foldLeft(Task.pure(Option.empty[Store[Doc, Model]])) { (task, shard) =>
//      task.flatMap { result =>
//        if (result.nonEmpty) {
//          Task.pure(result)
//        } else {
//          shard.delete(_ => field -> value).map {
//            case true => Some(shard)
//            case false => None
//          }
//        }
//      }
//    }
//    if (field == model._id) {
//      val id = value.asInstanceOf[Id[Doc]]
//      shardFor(id) match {
//        case Some(store) => store.delete(_ => field -> value).map {
//          case true => Some(store)
//          case false => None
//        }
//        case None => deleteFirst()
//      }
//    } else {
//      deleteFirst()
//    }
//  }
//
//  def exists(id: Id[Doc])(transaction: Transaction[Doc]): Task[Boolean] = shardFor(id) match {
//    case Some(store) => store.exists(id)
//    case None => firstMatch { store =>
//      store.exists(id).map {
//        case true => Some(true)
//        case false => None
//      }
//    }.map(_.getOrElse(false))
//  }
//
//  def reIndex(doc: Doc)(transaction: Transaction[Doc]): Task[Boolean] = shardFor(doc._id) match {
//    case Some(store) => store.reIndex(doc)
//    case None => findDocShard(doc._id).flatMap {
//      case Some(store) => store.reIndex(doc)
//      case None => Task.pure(false)
//    }
//  }
//}
