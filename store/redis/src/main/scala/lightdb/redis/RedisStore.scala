package lightdb.redis

import fabric.rw.RW
import lightdb.Id
import lightdb.document.{Document, SetType}
import lightdb.store.StringStore
import lightdb.transaction.Transaction
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

case class RedisStore[D <: Document[D]](hostname: String = "localhost",
                         port: Int = 6379)(implicit val rw: RW[D]) extends StringStore[D] {
  private lazy val pool = new JedisPool(new JedisPoolConfig, hostname, port)

  override def internalCounter: Boolean = ???

  override def idIterator(implicit transaction: Transaction[D]): Iterator[Id[D]] = ???

  override def iterator(implicit transaction: Transaction[D]): Iterator[D] = ???

  override def get(id: Id[D])(implicit transaction: Transaction[D]): Option[D] = ???

  override def put(id: Id[D], doc: D)(implicit transaction: Transaction[D]): Option[SetType] = ???

  override def delete(id: Id[D])(implicit transaction: Transaction[D]): Boolean = ???

  override def count(implicit transaction: Transaction[D]): Int = ???

  override def commit()(implicit transaction: Transaction[D]): Unit = ???

  override def dispose(): Unit = ???
}
