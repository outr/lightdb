package lightdb.doc

import fabric.rw._
import lightdb.field.Field

trait RecordDocumentModel[Doc <: RecordDocument[Doc]] extends DocumentModel[Doc] {
  val created: I[Long] = field.index("created", (doc: Doc) => doc.created)
  val modified: I[Long] = field.index("modified", (doc: Doc) => doc.modified)
}