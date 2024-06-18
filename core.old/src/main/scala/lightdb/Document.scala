package lightdb

trait Document[D <: Document[D]] {
  def _id: Id[D]
}