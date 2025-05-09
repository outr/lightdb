package lightdb.graph

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.field.Field.UniqueIndex
import lightdb.id.Id
import lightdb.store.{Store, StoreMode}
import lightdb.transaction.Transaction
import lightdb.trigger.StoreTrigger
import rapid.{Task, logger}

import scala.language.implicitConversions

trait EdgeModel[Doc <: EdgeDocument[Doc, From, To], From <: Document[From], To <: Document[To]] extends DocumentModel[Doc] {
  val _from: UniqueIndex[Doc, Id[From]] = field.unique("_from", _._from)
  val _to: UniqueIndex[Doc, Id[To]] = field.unique("_to", _._to)

  def prefix(_from: Id[From], _to: Id[To] = null, extra: String = null): String = if (_to == null) {
    _from.value
  } else if (extra == null) {
    s"${_from.value}-${_to.value}"
  } else {
    s"${_from.value}-${_to.value}-$extra"
  }
}
