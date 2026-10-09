package lightdb.store.journal

import fabric.rw.*
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id

/**
 * One commit of a journalled group: every op of the owner and the transactions joined to it, written as one record.
 * Its id is the commit's sequence number, zero-padded so the id order is the commit order.
 */
case class JournalIntent(ops: List[JournalOp], _id: Id[JournalIntent]) extends Document[JournalIntent] {
  def sequence: Long = _id.value.toLong
}

object JournalIntent extends DocumentModel[JournalIntent] with JsonConversion[JournalIntent] {
  override implicit val rw: RW[JournalIntent] = RW.gen

  def idFor(sequence: Long): Id[JournalIntent] = Id[JournalIntent](f"$sequence%019d")
}
