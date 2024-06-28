package lightdb.document
import lightdb.transaction.Transaction

trait RecordDocumentModel[D <: RecordDocument[D]] extends DocumentModel[D] {
  listener += new DocumentListener[D] {
    override def preSet(doc: D, transaction: Transaction[D]): Option[D] = super
      .preSet(doc, transaction)
      .map(doc => doc.updateModified())
  }
}