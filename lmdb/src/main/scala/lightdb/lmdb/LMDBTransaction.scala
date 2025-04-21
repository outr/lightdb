package lightdb.lmdb

import lightdb.doc.Document
import lightdb.store.WriteBuffer
import lightdb.transaction.TransactionFeature
import org.lmdbjava.{Env, Txn}
import rapid.Task

import java.nio.ByteBuffer

case class LMDBTransaction[Doc <: Document[Doc]](env: Env[ByteBuffer]) extends TransactionFeature {
  private var _readTxn: Txn[ByteBuffer] = env.txnRead()

  def readTxn: Txn[ByteBuffer] = _readTxn

  var writeBuffer: WriteBuffer[Doc] = WriteBuffer()

  override def commit(): Task[Unit] = Task {
//    scribe.info("COMMITTING!")
    _readTxn.close()
    _readTxn = env.txnRead()
  }.next(super.commit())

  override def close(): Task[Unit] = Task(readTxn.close())
}
