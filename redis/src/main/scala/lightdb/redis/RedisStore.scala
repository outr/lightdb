package lightdb.redis

import _root_.redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import lightdb._
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field._
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.{Transaction, TransactionKey}
import rapid.Task

import scala.jdk.CollectionConverters.IteratorHasAsScala

class RedisStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                    model: Model,
                                                                    val storeMode: StoreMode[Doc, Model],
                                                                    hostname: String = "localhost",
                                                                    port: Int = 6379,
                                                                    storeManager: StoreManager) extends Store[Doc, Model](name, model, storeManager) {
  private lazy val InstanceKey: TransactionKey[Jedis] = TransactionKey("redisInstance")

  private lazy val config = new JedisPoolConfig
  private lazy val pool = new JedisPool(config, hostname, port)

  override protected def initialize(): Task[Unit] = Task(pool.preparePool())

  private def getInstance(implicit transaction: Transaction[Doc]): Jedis =
    transaction.getOrCreate(InstanceKey, pool.getResource)

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = Task.unit

  override def releaseTransaction(transaction: Transaction[Doc]): Task[Unit] = Task {
    super.releaseTransaction(transaction)
    transaction.get(InstanceKey).foreach(jedis => pool.returnResource(jedis))
  }

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = upsert(doc)

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = Task {
    getInstance.hset(name, doc._id.value, toString(doc))
    doc
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] =
    Task(getInstance.hexists(name, id.value))

  override def get[V](field: UniqueIndex[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Task[Option[Doc]] = Task {
    if (field == idField) {
      Option(getInstance.hget(name, value.asInstanceOf[Id[Doc]].value)).map(fromString)
    } else {
      throw new UnsupportedOperationException(s"HaloDBStore can only get on _id, but ${field.name} was attempted")
    }
  }

  override def delete[V](field: UniqueIndex[Doc, V], value: V)
                        (implicit transaction: Transaction[Doc]): Task[Boolean] =
    Task(getInstance.hdel(value.asInstanceOf[Id[Doc]].value) > 0L)

  override def count(implicit transaction: Transaction[Doc]): Task[Int] = Task(getInstance.hlen(name).toInt)

  override def stream(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] = rapid.Stream.fromIterator(Task {
    getInstance.hgetAll(name).values().iterator().asScala.map(fromString)
  })

  override def doSearch[V](query: Query[Doc, Model, V])
                          (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]] =
    throw new UnsupportedOperationException("Redis does not support searching")

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    throw new UnsupportedOperationException("Redis does not support aggregation")

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Task[Int] =
    throw new UnsupportedOperationException("Redis does not support aggregation")

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = count.map { size =>
    getInstance.del(name)
    size
  }

  override protected def doDispose(): Task[Unit] = Task(pool.close())
}
