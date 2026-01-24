package lightdb.redis

import _root_.redis.clients.jedis.{JedisPool, JedisPoolConfig}
import lightdb.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import rapid.*

import java.nio.file.Path

class RedisStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                    path: Option[Path],
                                                                    model: Model,
                                                                    val storeMode: StoreMode[Doc, Model],
                                                                    hostname: String = "localhost",
                                                                    port: Int = 6379,
                                                                    db: LightDB,
                                                                    storeManager: StoreManager) extends Store[Doc, Model](name, path, model, db, storeManager) {
  override type TX = RedisTransaction[Doc, Model]

  private lazy val config = new JedisPoolConfig
  private[redis] lazy val pool = new JedisPool(config, hostname, port)

  override protected def initialize(): Task[Unit] = super.initialize().next(Task(pool.preparePool()))

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]]): Task[TX] = Task {
    RedisTransaction(this, pool.getResource, parent)
  }

  override protected def doDispose(): Task[Unit] = super.doDispose().next(Task(pool.close()))
}
