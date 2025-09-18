package lightdb.doc

import lightdb.time.Timestamp

trait RecordDocument[Doc <: RecordDocument[Doc]] extends Document[Doc] {
  def created: Timestamp
  def modified: Timestamp
}
