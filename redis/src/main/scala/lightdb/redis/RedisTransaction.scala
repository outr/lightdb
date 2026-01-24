package lightdb.redis

import fabric.Json
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.transaction.Transaction
import rapid.Task
import redis.clients.jedis.Jedis

import scala.jdk.CollectionConverters.*

case class RedisTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: RedisStore[Doc, Model],
                                                                               jedis: Jedis,
                                                                               parent: Option[Transaction[Doc, Model]]) extends Transaction[Doc, Model] {
  override def jsonStream: rapid.Stream[Json] = rapid.Stream.fromIterator(Task {
    jedis.hgetAll(store.name).values().iterator().asScala.map(toJson)
  })

  override protected def _get[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = Task {
    if index == store.idField then {
      Option(jedis.hget(store.name, value.asInstanceOf[Id[Doc]].value)).map(fromString)
    } else {
      throw new UnsupportedOperationException(s"RedisStore can only get on _id, but ${index.name} was attempted")
    }
  }

  override protected def _insert(doc: Doc): Task[Doc] = _upsert(doc)

  override protected def _upsert(doc: Doc): Task[Doc] = Task {
    jedis.hset(store.name, doc._id.value, toString(doc))
    doc
  }

  override protected def _exists(id: Id[Doc]): Task[Boolean] = Task(jedis.hexists(store.name, id.value))

  override protected def _count: Task[Int] = Task(jedis.hlen(store.name).toInt)

  override protected def _delete[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Boolean] =
    Task(jedis.hdel(value.asInstanceOf[Id[Doc]].value) > 0L)

  override protected def _commit: Task[Unit] = Task.unit

  override protected def _rollback: Task[Unit] = Task.unit

  override protected def _close: Task[Unit] = Task {
    store.pool.returnResource(jedis)
  }

  override def truncate: Task[Int] = count.map { size =>
    jedis.del(store.name)
    size
  }
}
