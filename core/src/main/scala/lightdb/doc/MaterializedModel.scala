package lightdb.doc

import lightdb.Id
import lightdb.collection.Collection
import lightdb.transaction.Transaction
import lightdb.trigger.BasicCollectionTrigger
import rapid.Task

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

  override def init[Model <: DocumentModel[Doc]](collection: Collection[Doc, Model]): Task[Unit] = {
    super.init(collection).map { _ =>
      materialCollection.trigger += new BasicCollectionTrigger[MaterialDoc, MaterialModel] {
        override def collection: Collection[MaterialDoc, MaterialModel] = materialCollection

        override protected def adding(doc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Task[Unit] = Task(mm.adding(doc))
        override protected def modifying(oldDoc: MaterialDoc, newDoc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Task[Unit] = Task(mm.modifying(oldDoc, newDoc))
        override protected def removing(doc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc]): Task[Unit] = Task(mm.removing(doc))
        override def transactionStart(transaction: Transaction[MaterialDoc]): Task[Unit] = Task(mm.transactionStart(transaction))
        override def transactionEnd(transaction: Transaction[MaterialDoc]): Task[Unit] = Task(mm.transactionEnd(transaction))
      }
    }
  }
}