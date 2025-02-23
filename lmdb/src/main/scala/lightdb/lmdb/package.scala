package lightdb

import lightdb.doc.Document
import lightdb.transaction.{Transaction, TransactionKey}
import org.lmdbjava.Txn

import java.nio.ByteBuffer

package object lmdb {
  val StateKey: TransactionKey[LMDBTransaction] = TransactionKey("lmdbTxn")

  def getState[Doc <: Document[Doc]](implicit transaction: Transaction[Doc]): LMDBTransaction = transaction(StateKey)
}
