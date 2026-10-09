package lightdb.store.journal

import lightdb.doc.{Document, DocumentModel}
import lightdb.transaction.{Transaction, WriteHandler}

case class JournalTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  store: JournalStore[Doc, Model],
  parent: Option[Transaction[Doc, Model]],
  writeHandlerFactory: Transaction[Doc, Model] => WriteHandler[Doc, Model]
) extends JournalTransactionSupport[Doc, Model] {
  override lazy val writeHandler: WriteHandler[Doc, Model] = writeHandlerFactory(this)

  override def journalStore: JournalBacked[Doc, Model] = store
}
