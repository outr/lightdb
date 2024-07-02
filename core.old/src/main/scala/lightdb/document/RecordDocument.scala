package lightdb.document

trait RecordDocument[D <: RecordDocument[D]] extends Document[D] {
  def created: Long
  def modified: Long

  def updateModified(): D
}