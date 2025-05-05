package lightdb.transaction

import fabric._
import fabric.rw._
import lightdb.doc.{Document, DocumentModel}
import rapid.Task

trait PrefixScanningTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Transaction[Doc, Model] {
  def jsonPrefixStream(prefix: String): rapid.Stream[Json]
  def prefixStream(prefix: String): rapid.Stream[Doc] = jsonPrefixStream(prefix).map(_.as[Doc](store.model.rw))
  def prefixList(prefix: String): Task[List[Doc]] = prefixStream(prefix).toList
}
