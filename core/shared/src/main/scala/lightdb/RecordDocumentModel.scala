package lightdb

import lightdb.field.Field

trait RecordDocumentModel[D <: RecordDocument[D]] extends DocumentModel[D] {
  val created: Field[D, Long] = field("created", _.created)
  val modified: Field[D, Long] = field("modified", _.modified)
}