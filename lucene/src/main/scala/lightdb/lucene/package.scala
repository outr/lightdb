package lightdb

import lightdb.doc.Document
import lightdb.transaction.{Transaction, TransactionKey}

package object lucene {
  def StateKey[Doc <: Document[Doc]]: TransactionKey[LuceneState[Doc]] = TransactionKey("luceneState")

  def state[Doc <: Document[Doc]](implicit transaction: Transaction[Doc]): LuceneState[Doc] = transaction(StateKey[Doc])
}
