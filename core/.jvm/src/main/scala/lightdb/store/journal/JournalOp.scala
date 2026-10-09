package lightdb.store.journal

import fabric.Json
import fabric.rw.*

/** One op of a commit-intent record: `doc` for a put, `id` for a put or delete. */
case class JournalOp(store: String, kind: JournalOpKind, id: Option[String], doc: Option[Json])

object JournalOp {
  implicit val rw: RW[JournalOp] = RW.gen
}
