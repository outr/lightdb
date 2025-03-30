package lightdb.redis

import _root_.redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import fabric.Json
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
                                                                    db: LightDB,
                                                                    storeManager: StoreManager) extends Store[Doc, Model](name, model, db, storeManager) {
  private lazy val InstanceKey: TransactionKey[Jedis] = TransactionKey("redisInstance")

  private lazy val config = new JedisPoolConfig
  private lazy val pool = new JedisPool(config, hostname, port)

  override protected def initialize(): Task[Unit] = super.initialize().next(Task(pool.preparePool()))

  private def getInstance(implicit transaction: Transaction[Doc]): Jedis =
    transaction.getOrCreate(InstanceKey, pool.getResource)

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = Task.unit

  override def releaseTransaction(transaction: Transaction[Doc]): Task[Unit] = Task {
    super.releaseTransaction(transaction)
    transaction.get(InstanceKey).foreach(jedis => pool.returnResource(jedis))
  }

  override protected def _insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = upsert(doc)

  override protected def _upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = Task {
    getInstance.hset(name, doc._id.value, toString(doc))
    doc
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] =
    Task(getInstance.hexists(name, id.value))

  override protected def _get[V](field: UniqueIndex[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Task[Option[Doc]] = Task {
    if (field == idField) {
      Option(getInstance.hget(name, value.asInstanceOf[Id[Doc]].value)).map(fromString)
    } else {
      throw new UnsupportedOperationException(s"HaloDBStore can only get on _id, but ${field.name} was attempted")
    }
  }

  override protected def _delete[V](field: UniqueIndex[Doc, V], value: V)
                        (implicit transaction: Transaction[Doc]): Task[Boolean] =
    Task(getInstance.hdel(value.asInstanceOf[Id[Doc]].value) > 0L)

  override def count(implicit transaction: Transaction[Doc]): Task[Int] = Task(getInstance.hlen(name).toInt)

  override def jsonStream(implicit transaction: Transaction[Doc]): rapid.Stream[Json] = rapid.Stream.fromIterator(Task {
    getInstance.hgetAll(name).values().iterator().asScala.map(toJson)
  })

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = count.map { size =>
    getInstance.del(name)
    size
  }

  override protected def doDispose(): Task[Unit] = super.doDispose().next(Task(pool.close()))
}
