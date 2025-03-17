package lightdb.store.sharded.manager

import lightdb.Id
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.store.Store
import lightdb.transaction.Transaction
import rapid._

import java.util.concurrent.atomic.AtomicInteger

case class BalancedShardManager[Doc <: Document[Doc], Model <: DocumentModel[Doc]](model: Model,
                                                                                   shards: Vector[Store[Doc, Model]]) extends ShardManagerInstance[Doc, Model] {
  private lazy val counters: Vector[AtomicInteger] = shards.map { store =>
    store.transaction { implicit transaction =>
      store.count
    }
  }.tasks.map { counts =>
    counts.map { count =>
      new AtomicInteger(count)
    }
  }.sync()

  private def updateCounterFor(shard: Store[Doc, Model], delta: Int)(implicit transaction: Transaction[Doc]): Task[Unit] = Task.defer {
    val index = shards.indexOf(shard)
    counters(index).addAndGet(delta)
    Task.unit
  }

  private def nextShard(): Task[Store[Doc, Model]] = Task {
    val leastIndex = counters.zipWithIndex.map {
      case (counter, index) => counter.get() -> index
    }.minBy(_._1)._2
    shards(leastIndex)
  }

  override def shardFor(id: Id[Doc]): Option[Store[Doc, Model]] = None

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = {
    nextShard().flatMap { shard =>
      shard.insert(doc).flatTap { _ =>
        updateCounterFor(shard, 1)
      }
    }
  }

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = {
    nextShard().flatMap { shard =>
      shard.upsert(doc).flatTap { _ =>
        updateCounterFor(shard, 1)
      }
    }
  }

  override def delete[V](field: Field.UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Option[Store[Doc, Model]]] = super.delete(field, value).map {
    case Some(store) =>
      updateCounterFor(store, -1)
      Some(store)
    case None => None
  }
}

object BalancedShardManager extends ShardManager {
  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](model: Model, shards: Vector[Store[Doc, Model]]): ShardManagerInstance[Doc, Model] =
    BalancedShardManager(model, shards)
}