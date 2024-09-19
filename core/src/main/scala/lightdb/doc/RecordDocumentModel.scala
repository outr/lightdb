package lightdb.doc

import fabric.rw._
import lightdb.field.Field

trait RecordDocumentModel[Doc <: RecordDocument[Doc]] extends DocumentModel[Doc] {
  protected def indexCreated: Boolean = false
  protected def indexModified: Boolean = false

  val created: Field[Doc, Long] = if (indexCreated) {
    field.index("created", (doc: Doc) => doc.created)
  } else {
    field("created", (doc: Doc) => doc.created)
  }

  val modified: Field[Doc, Long] = if (indexModified) {
    field.index("modified", (doc: Doc) => doc.modified)
  } else {
    field("modified", (doc: Doc) => doc.modified)
  }
}
