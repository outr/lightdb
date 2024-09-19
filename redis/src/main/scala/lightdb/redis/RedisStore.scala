package lightdb.redis

import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb.{Field, Id, Query, SearchResults, UniqueIndex}
import lightdb.doc.{Document, DocumentModel}
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Conversion, Store, StoreMode}
import lightdb.transaction.{Transaction, TransactionKey}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.jdk.CollectionConverters.IteratorHasAsScala

class RedisStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](val storeMode: StoreMode,
                                                                    hostname: String = "localhost",
                                                                    port: Int = 6379) extends Store[Doc, Model] {
  private lazy val InstanceKey: TransactionKey[Jedis] = TransactionKey("redisInstance")

  private lazy val config = new JedisPoolConfig
  private lazy val pool = new JedisPool(config, hostname, port)

  override def init(collection: Collection[Doc, Model]): Unit = {
    super.init(collection)

    pool.preparePool()
  }

  private def getInstance(implicit transaction: Transaction[Doc]): Jedis =
    transaction.getOrCreate(InstanceKey, pool.getResource)

  override def prepareTransaction(transaction: Transaction[Doc]): Unit = ()

  override def releaseTransaction(transaction: Transaction[Doc]): Unit = {
    super.releaseTransaction(transaction)
    transaction.get(InstanceKey).foreach(jedis => pool.returnResource(jedis))
  }

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = upsert(doc)

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit =
    getInstance.hset(collection.name, doc._id.value, toString(doc))

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Boolean =
    getInstance.hexists(collection.name, id.value)

  override def get[V](field: UniqueIndex[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Option[Doc] = {
    if (field == idField) {
      Option(getInstance.hget(collection.name, value.asInstanceOf[Id[Doc]].value)).map(fromString)
    } else {
      throw new UnsupportedOperationException(s"HaloDBStore can only get on _id, but ${field.name} was attempted")
    }
  }

  override def delete[V](field: UniqueIndex[Doc, V], value: V)
                        (implicit transaction: Transaction[Doc]): Boolean =
    getInstance.hdel(value.asInstanceOf[Id[Doc]].value) > 0L

  override def count(implicit transaction: Transaction[Doc]): Int = getInstance.hlen(collection.name).toInt

  override def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc] = getInstance.hgetAll(collection.name)
    .values().iterator().asScala.map(fromString)

  override def doSearch[V](query: Query[Doc, Model], conversion: Conversion[Doc, V])
                          (implicit transaction: Transaction[Doc]): SearchResults[Doc, Model, V] =
    throw new UnsupportedOperationException("Redis does not support searching")

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): Iterator[MaterializedAggregate[Doc, Model]] =
    throw new UnsupportedOperationException("Redis does not support aggregation")

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Int =
    throw new UnsupportedOperationException("Redis does not support aggregation")

  override def truncate()(implicit transaction: Transaction[Doc]): Int = {
    val size = count
    getInstance.del(collection.name)
    size
  }

  override def dispose(): Unit = pool.close()
}
