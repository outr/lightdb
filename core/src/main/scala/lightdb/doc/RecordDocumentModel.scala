package lightdb.doc

import fabric.rw._
import lightdb.Timestamp

trait RecordDocumentModel[Doc <: RecordDocument[Doc]] extends DocumentModel[Doc] {
  val created: I[Timestamp] = field.index("created", (doc: Doc) => doc.created)
  val modified: I[Timestamp] = field.index("modified", (doc: Doc) => doc.modified)
}