package lightdb.doc

import lightdb.Id
import lightdb.collection.Collection
import lightdb.transaction.Transaction
import lightdb.trigger.BasicCollectionTrigger

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

trait MaterializedModel[Doc <: Document[Doc], MaterialDoc <: Document[MaterialDoc], MaterialModel <: DocumentModel[MaterialDoc]] extends DocumentModel[Doc] { mm =>
  def materialCollection: Collection[MaterialDoc, MaterialModel]

  protected def adding(doc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Unit
  protected def modifying(oldDoc: MaterialDoc, newDoc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Unit
  protected def removing(doc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Unit
  protected def transactionStart(transaction: Transaction[MaterialDoc]): Unit
  protected def transactionEnd(transaction: Transaction[MaterialDoc]): Unit

  override def init[Model <: DocumentModel[Doc]](collection: Collection[Doc, Model]): Unit = {
    super.init(collection)

    materialCollection.trigger += new BasicCollectionTrigger[MaterialDoc, MaterialModel] {
      override def collection: Collection[MaterialDoc, MaterialModel] = materialCollection

      override protected def adding(doc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Unit = mm.adding(doc)
      override protected def modifying(oldDoc: MaterialDoc, newDoc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Unit = mm.modifying(oldDoc, newDoc)
      override protected def removing(doc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Unit = mm.removing(doc)
      override def transactionStart(transaction: Transaction[MaterialDoc]): Unit = mm.transactionStart(transaction)
      override def transactionEnd(transaction: Transaction[MaterialDoc]): Unit = mm.transactionEnd(transaction)
    }
  }
}