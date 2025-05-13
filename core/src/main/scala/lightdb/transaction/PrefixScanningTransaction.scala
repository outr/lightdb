package lightdb.transaction

import fabric._
import fabric.rw._
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.PrefixScanningStore
import lightdb.traversal.{TransactionTraversalSupport => TransactionTraversalSupportOld}
import lightdb.traverse.TransactionTraversalSupport

trait PrefixScanningTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Transaction[Doc, Model] with TransactionTraversalSupportOld[Doc, Model] with TransactionTraversalSupport[Doc, Model] {
  override def store: PrefixScanningStore[Doc, Model]

  def jsonPrefixStream(prefix: String): rapid.Stream[Json]

  def prefixStream(prefix: String): rapid.Stream[Doc] = jsonPrefixStream(prefix).map(_.as[Doc](store.model.rw))
}
