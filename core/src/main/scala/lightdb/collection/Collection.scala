package lightdb.collection

import lightdb.{Id, LightDB}
import lightdb.document.{Document, DocumentListener, DocumentModel}
import lightdb.transaction.Transaction
import lightdb.util.Initializable
import fabric.Json
import fabric.rw.{Convertible, RW}
import lightdb.store.Store

import scala.annotation.tailrec
import scala.concurrent.duration.DurationInt

class Collection[D <: Document[D], M <: DocumentModel[D]](val name: String,
                                                          val model: M,
                                                          val db: LightDB)
                                                         (implicit rw: RW[D]) extends Initializable { collection =>
  private[lightdb] lazy val store: Store = db.storeManager(db, name)

  private def recurseOption(doc: D,
                            invoke: (DocumentListener[D], D) => Option[D],
                            listeners: List[DocumentListener[D]] = model.listener()): Option[D] = listeners.headOption match {
    case Some(l) => invoke(l, doc) match {
      case Some(v) => recurseOption(v, invoke, listeners.tail)
      case None => None
    }
    case None => Some(doc)
  }

  override protected def initialize(): Unit = {
    model.collection = this
    model.listener().map(_.init(this)).foreach(_ => model._initialized.set(true))
  }

  object transaction {
    private var _active = Set.empty[Transaction[D]]
    def active: Set[Transaction[D]] = _active

    private def add(transaction: Transaction[D]): Unit = synchronized(_active += transaction)
    private def remove(transaction: Transaction[D]): Unit = synchronized(_active -= transaction)

    def apply[Return](f: Transaction[D] => Return): Return = {
      val transaction = create()
      try {
        f(transaction)
      } finally {
        release(transaction)
      }
    }

    def create(): Transaction[D] = {
      val transaction = Transaction[D](collection)
      model.listener().foreach(l => l.transactionStart(transaction))
      add(transaction)
      transaction
    }

    def release(transaction: Transaction[D]): Unit = {
      transaction.commit()
      model.listener().foreach(l => l.transactionEnd(transaction))
      remove(transaction)
    }
  }

  def apply(id: Id[D])(implicit transaction: Transaction[D]): D = get(id)
    .getOrElse(throw new RuntimeException(s"$id not found in $name"))

  def get(id: Id[D])(implicit transaction: Transaction[D]): Option[D] = model.store.getJsonDoc(id)(rw)

  final def set(doc: D)(implicit transaction: Transaction[D]): Option[D] = {
    recurseOption(doc, (l, d) => l.preSet(d, transaction)) match {
      case Some(d) =>
        model.store.putJson(d._id, d.json)
        model.listener().foreach(l => l.postSet(d, transaction))
        Some(d)
      case None => None
    }
  }

  def set(iterator: Iterator[D])(implicit transaction: Transaction[D]): Int = iterator.map(set).size

  def set(docs: Seq[D])(implicit transaction: Transaction[D]): Int = set(docs.iterator)

  def modify(id: Id[D], lock: Boolean = true, deleteOnNone: Boolean = false)
            (f: Option[D] => Option[D])
            (implicit transaction: Transaction[D]): Option[D] = transaction.mayLock(id, lock) {
    f(get(id)) match {
      case Some(doc) => set(doc)
      case None if deleteOnNone =>
        delete(id)
        None
      case None => None
    }
  }

  def iterator(implicit transaction: Transaction[D]): Iterator[D] = model.store.streamJsonDocs[D]

  def count(implicit transaction: Transaction[D]): Int = model.store.count

  def idIterator(implicit transaction: Transaction[D]): Iterator[Id[D]] = model.store.keyStream[D]

  def jsonIterator(implicit transaction: Transaction[D]): Iterator[Json] = iterator.map(_.json)

  private[lightdb] def commit(transaction: Transaction[D]): Unit = model.listener()
    .foreach(l => l.commit(transaction))

  private[lightdb] def rollback(transaction: Transaction[D]): Unit = model.listener()
    .foreach(l => l.rollback(transaction))

  final def delete(doc: D)(implicit transaction: Transaction[D]): Option[D] = {
    recurseOption(doc, (l, d) => l.preDelete(d, transaction)) match {
      case Some(d) =>
        model.store.delete(d._id)
        model.listener().foreach(l => l.postDelete(d, transaction))
        Some(d)
      case None => None
    }
  }

  def delete(iterator: Iterator[D])(implicit transaction: Transaction[D]): Int = iterator.map(delete).size

  def delete(docs: Seq[D])(implicit transaction: Transaction[D]): Int = delete(docs.iterator)

  def delete(id: Id[D])(implicit transaction: Transaction[D]): Option[D] = get(id) match {
      case Some(doc) => delete(doc)
      case None => None
    }

  def truncate()(implicit transaction: Transaction[D]): Int = {
    val count = model.store.count
    model.store.truncate()
    model.listener().foreach(l => l.truncate(transaction))
    count
  }

  def update(): Unit = ()

  @tailrec
  final def dispose(): Unit = if (transaction.active.isEmpty) {
    model.listener().foreach(l => l.dispose())
    store.dispose()
  } else {
    scribe.warn(s"Waiting to dispose $name. ${transaction.active.size} transactions are still active...")
    Thread.sleep(1.second.toMillis)
    dispose()
  }
}