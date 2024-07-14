package lightdb

import lightdb.doc.Document
import lightdb.transaction.{Transaction, TransactionKey}

package object sql {
  def StateKey[Doc <: Document[Doc]]: TransactionKey[SQLState[Doc]] = TransactionKey("sqlState")

  def getState[Doc <: Document[Doc]](implicit transaction: Transaction[Doc]): SQLState[Doc] = transaction(StateKey[Doc])
}