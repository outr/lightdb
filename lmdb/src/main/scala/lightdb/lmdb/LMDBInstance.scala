package lightdb.lmdb

import org.lmdbjava.{Dbi, DbiFlags, Env}
import rapid.Task

import java.nio.ByteBuffer

case class LMDBInstance(env: Env[ByteBuffer]) {
  instance =>
  private var map = Map.empty[String, Dbi[ByteBuffer]]

  def createTransaction(): LMDBTransaction = LMDBTransaction(env.txnWrite())

  def get(name: String): Dbi[ByteBuffer] = synchronized {
    map.get(name) match {
      case Some(dbi) => dbi
      case None =>
        val dbi = env.openDbi(name, DbiFlags.MDB_CREATE)
        map += name -> dbi
        dbi
    }
  }

  def release(name: String): Task[Unit] = Task {
    instance.synchronized {
      map.get(name) match {
        case Some(dbi) =>
          map -= name
          dbi.close()
          if (map.isEmpty) {
            env.close()
          }
        case None => // Ignore
      }
    }
  }
}
