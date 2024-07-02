package lightdb.doc

trait RecordDocument[Doc <: RecordDocument[Doc]] extends Document[Doc] {
  def created: Long
  def modified: Long
}
