package lightdb.redis

import fabric.rw.RW
import lightdb.{Id, LightDB}
import lightdb.document.{Document, SetType}
import lightdb.store.{Store, StoreManager, StringStore}
import lightdb.transaction.{Transaction, TransactionKey}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import java.util
import scala.jdk.CollectionConverters._

case class RedisStore[D <: Document[D]](hostname: String = "localhost",
                         port: Int = 6379)(implicit val rw: RW[D]) extends StringStore[D] {
  private lazy val instanceKey: TransactionKey[Jedis] = TransactionKey("redisInstance")

  private lazy val config = new JedisPoolConfig
  private lazy val pool = new JedisPool(config, hostname, port)

  private def getInstance()(implicit transaction: Transaction[D]): Jedis =
    transaction.getOrCreate(instanceKey, pool.getResource)

  private def releaseInstance()(implicit transaction: Transaction[D]): Unit = transaction
    .get(instanceKey)
    .foreach { jedis =>
      pool.returnResource(jedis)
    }

  override def internalCounter: Boolean = true

  override def idIterator(implicit transaction: Transaction[D]): Iterator[Id[D]] =
    getInstance().hgetAll(transaction.collection.name).keySet().asScala.iterator.map(key => Id[D](key))

  override def iterator(implicit transaction: Transaction[D]): Iterator[D] =
    getInstance().hgetAll(transaction.collection.name).values().asScala.iterator.map(string2D)

  override def get(id: Id[D])(implicit transaction: Transaction[D]): Option[D] =
    Option(getInstance().hget(transaction.collection.name, id.value)).map(string2D)

  override def put(id: Id[D], doc: D)(implicit transaction: Transaction[D]): Option[SetType] = {
    if (getInstance().hset(transaction.collection.name, id.value, d2String(doc)) == 0L) {
      Some(SetType.Replace)
    } else {
      Some(SetType.Insert)
    }
  }

  override def delete(id: Id[D])(implicit transaction: Transaction[D]): Boolean =
    getInstance().hdel(transaction.collection.name, id.value) > 0L

  override def count(implicit transaction: Transaction[D]): Int =
    getInstance().hlen(transaction.collection.name).toInt

  override def commit()(implicit transaction: Transaction[D]): Unit = ()

  override def transactionEnd()(implicit transaction: Transaction[D]): Unit = releaseInstance()

  override def truncate()(implicit transaction: Transaction[D]): Unit =
    getInstance().del(transaction.collection.name)

  override def dispose(): Unit = pool.close()
}

object RedisStore extends StoreManager {
  override protected def create[D <: Document[D]](db: LightDB, name: String)(implicit rw: RW[D]): Store[D] = new RedisStore[D]()
}