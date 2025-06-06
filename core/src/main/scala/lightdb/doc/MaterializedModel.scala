package lightdb.doc

import lightdb.store.Store
import lightdb.transaction.Transaction
import lightdb.trigger.BasicStoreTrigger
import rapid.Task

import scala.annotation.unchecked.uncheckedVariance

trait MaterializedModel[Doc <: Document[Doc], MaterialDoc <: Document[MaterialDoc], MaterialModel <: DocumentModel[MaterialDoc]] extends DocumentModel[Doc] { mm =>
  def materialStore: Store[MaterialDoc, MaterialModel @uncheckedVariance]

  protected def adding(doc: MaterialDoc, transaction: Transaction[MaterialDoc, MaterialModel]): Task[Unit]
  protected def modifying(oldDoc: MaterialDoc, newDoc: MaterialDoc, transaction: Transaction[MaterialDoc, MaterialModel]): Task[Unit]
  protected def removing(doc: MaterialDoc, transaction: Transaction[MaterialDoc, MaterialModel]): Task[Unit]
  protected def transactionStart(transaction: Transaction[MaterialDoc, MaterialModel]): Task[Unit]
  protected def transactionEnd(transaction: Transaction[MaterialDoc, MaterialModel]): Task[Unit]

  override protected def init[Model <: DocumentModel[Doc]](store: Store[Doc, Model]): Task[Unit] = {
    super.initialize(store).map { _ =>
      materialStore.trigger += new BasicStoreTrigger[MaterialDoc, MaterialModel] {
        override def store: Store[MaterialDoc, MaterialModel] = materialStore

        override protected def adding(doc: MaterialDoc, transaction: Transaction[MaterialDoc, MaterialModel]): Task[Unit] = mm.adding(doc, transaction)
        override protected def modifying(oldDoc: MaterialDoc, newDoc: MaterialDoc, transaction: Transaction[MaterialDoc, MaterialModel]): Task[Unit] = mm.modifying(oldDoc, newDoc, transaction)
        override protected def removing(doc: MaterialDoc, transaction: Transaction[MaterialDoc, MaterialModel]): Task[Unit] = mm.removing(doc, transaction)
        override def transactionStart(transaction: Transaction[MaterialDoc, MaterialModel]): Task[Unit] = mm.transactionStart(transaction)
        override def transactionEnd(transaction: Transaction[MaterialDoc, MaterialModel]): Task[Unit] = mm.transactionEnd(transaction)
      }
    }
  }
}
