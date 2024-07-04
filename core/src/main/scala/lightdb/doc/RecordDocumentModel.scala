package lightdb.doc

import lightdb.Field
import fabric.rw._

trait RecordDocumentModel[Doc <: RecordDocument[Doc]] extends DocumentModel[Doc] {
  protected def indexCreated: Boolean = false
  protected def indexModified: Boolean = false

  val created: Field[Doc, Long] = if (indexCreated) {
    field.index("created", _.created)
  } else {
    field("created", _.created)
  }

  val modified: Field[Doc, Long] = if (indexModified) {
    field.index("modified", _.modified)
  } else {
    field("modified", _.modified)
  }
}
