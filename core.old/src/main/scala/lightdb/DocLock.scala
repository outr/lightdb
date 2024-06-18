package lightdb

sealed trait DocLock[D <: Document[D]] extends Any

object DocLock {
  case class Set[D <: Document[D]](id: Id[D]) extends DocLock[D]
  class Empty[D <: Document[D]] extends DocLock[D]
}