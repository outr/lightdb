package lightdb.doc

import lightdb.Timestamp

trait RecordDocument[Doc <: RecordDocument[Doc]] extends Document[Doc] {
  def created: Timestamp
  def modified: Timestamp
}
