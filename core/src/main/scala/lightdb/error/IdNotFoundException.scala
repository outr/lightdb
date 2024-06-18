package lightdb.error

import lightdb.Id
import lightdb.document.Document

case class IdNotFoundException[D <: Document[D]](id: Id[D]) extends RuntimeException(s"$id not found")
