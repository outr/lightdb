package lightdb.doc

import lightdb.store.Store
import lightdb.transaction.Transaction
import lightdb.trigger.BasicCollectionTrigger
import rapid.Task

trait MaterializedModel[Doc <: Document[Doc], MaterialDoc <: Document[MaterialDoc], MaterialModel <: DocumentModel[MaterialDoc]] extends DocumentModel[Doc] { mm =>
  def materialStore: Store[MaterialDoc, MaterialModel]

  protected def adding(doc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Task[Unit]
  protected def modifying(oldDoc: MaterialDoc, newDoc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Task[Unit]
  protected def removing(doc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Task[Unit]
  protected def transactionStart(transaction: Transaction[MaterialDoc]): Task[Unit]
  protected def transactionEnd(transaction: Transaction[MaterialDoc]): Task[Unit]

  override protected def init[Model <: DocumentModel[Doc]](store: Store[Doc, Model]): Task[Unit] = {
    super.initialize(store).map { _ =>
      materialStore.trigger += new BasicCollectionTrigger[MaterialDoc, MaterialModel] {
        override def store: Store[MaterialDoc, MaterialModel] = materialStore

        override protected def adding(doc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Task[Unit] = mm.adding(doc)
        override protected def modifying(oldDoc: MaterialDoc, newDoc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Task[Unit] = mm.modifying(oldDoc, newDoc)
        override protected def removing(doc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Task[Unit] = mm.removing(doc)
        override def transactionStart(transaction: Transaction[MaterialDoc]): Task[Unit] = mm.transactionStart(transaction)
        override def transactionEnd(transaction: Transaction[MaterialDoc]): Task[Unit] = mm.transactionEnd(transaction)
      }
    }
  }
}