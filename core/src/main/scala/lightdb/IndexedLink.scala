package lightdb

import fabric.rw.RW

case class IndexedLink[D <: Document[D]](_id: Id[IndexedLink[D]],
                                         links: List[Id[D]]) extends Document[IndexedLink[D]]

object IndexedLink {
  implicit def rw[D <: Document[D]]: RW[IndexedLink[D]] = RW.gen
}