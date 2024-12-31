package lightdb.doc

import lightdb.collection.Collection
import lightdb.transaction.Transaction
import lightdb.trigger.BasicCollectionTrigger
import rapid.Task

trait MaterializedModel[Doc <: Document[Doc], MaterialDoc <: Document[MaterialDoc], MaterialModel <: DocumentModel[MaterialDoc]] extends DocumentModel[Doc] { mm =>
  def materialCollection: Collection[MaterialDoc, MaterialModel]

  protected def adding(doc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Task[Unit]
  protected def modifying(oldDoc: MaterialDoc, newDoc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Task[Unit]
  protected def removing(doc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Task[Unit]
  protected def transactionStart(transaction: Transaction[MaterialDoc]): Task[Unit]
  protected def transactionEnd(transaction: Transaction[MaterialDoc]): Task[Unit]

  override def init[Model <: DocumentModel[Doc]](collection: Collection[Doc, Model]): Task[Unit] = {
    super.init(collection).map { _ =>
      materialCollection.trigger += new BasicCollectionTrigger[MaterialDoc, MaterialModel] {
        override def collection: Collection[MaterialDoc, MaterialModel] = materialCollection

        override protected def adding(doc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Task[Unit] = mm.adding(doc)
        override protected def modifying(oldDoc: MaterialDoc, newDoc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Task[Unit] = mm.modifying(oldDoc, newDoc)
        override protected def removing(doc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Task[Unit] = mm.removing(doc)
        override def transactionStart(transaction: Transaction[MaterialDoc]): Task[Unit] = mm.transactionStart(transaction)
        override def transactionEnd(transaction: Transaction[MaterialDoc]): Task[Unit] = mm.transactionEnd(transaction)
      }
    }
  }
}