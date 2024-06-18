package lightdb

trait RecordDocument[D <: RecordDocument[D]] extends Document[D] {
  def created: Long
  def modified: Long
}