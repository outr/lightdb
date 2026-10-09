package lightdb.store.journal

import fabric.rw.RW

/** What one op of a commit-intent record does to its store. */
sealed trait JournalOpKind

object JournalOpKind {
  implicit val rw: RW[JournalOpKind] = RW.enumeration(List(Put, Delete, Truncate))

  /** Write the document under its id (an insert or upsert; applying it again leaves the same row). */
  case object Put extends JournalOpKind
  /** Remove the id. */
  case object Delete extends JournalOpKind
  /** Remove every row; applied before the store's other ops in the same intent. */
  case object Truncate extends JournalOpKind
}
