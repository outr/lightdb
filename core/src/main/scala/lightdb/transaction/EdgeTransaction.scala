package lightdb.transaction

import lightdb.doc.{Document, DocumentModel}
import lightdb.graph.{EdgeDocument, EdgeModel}
import lightdb.id.Id

case class EdgeTransaction[
  Doc <: EdgeDocument[Doc, From, To],
  Model <: EdgeModel[Doc, From, To],
  From <: Document[From],
  To <: Document[To]
](t: PrefixScanningTransaction[Doc, Model]) {
  def edgesFor(from: Id[From]): rapid.Stream[Doc] = ???
}
