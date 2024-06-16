package lightdb.document

import cats.effect.IO
import cats.implicits.{catsSyntaxParallelSequence1, toTraverseOps}
import lightdb.util.Unique
import lightdb.Id
import lightdb.collection.Collection
import lightdb.store.Store
import lightdb.transaction.Transaction

import java.util.concurrent.atomic.AtomicBoolean

trait DocumentModel[D <: Document[D]] {
  private val _initialized = new AtomicBoolean(false)
  private var collection: Collection[D] = _
  private var store: Store[D] = _

  final def initialized: Boolean = _initialized.get()

  protected def parallel: Boolean = true

  object listener {
    private var list = List.empty[DocumentListener[D]]

    def +=(listener: DocumentListener[D]): Unit = synchronized {
      if (initialized) throw new RuntimeException("Already initialized. Listeners should be added before initialization")
      list = (list ::: List(listener)).sortBy(_.priority)
    }

    def -=(listener: DocumentListener[D]): Unit = synchronized {
      list = list.filterNot(_ eq listener)
    }

    def apply(): List[DocumentListener[D]] = list
  }

  private implicit class ListIO[R](list: List[IO[R]]) {
    def ioSeq: IO[Unit] = if (parallel) {
      list.parSequence.map(_ => ())
    } else {
      list.sequence.map(_ => ())
    }
  }

  private def recurseOption(doc: D,
                            invoke: (DocumentListener[D], D) => IO[Option[D]],
                            listeners: List[DocumentListener[D]] = listener()): IO[Option[D]] = listeners.headOption match {
    case Some(l) => invoke(l, doc).flatMap {
      case Some(v) => recurseOption(v, invoke, listeners.tail)
      case None => IO.pure(None)
    }
    case None => IO.pure(Some(doc))
  }

  private[lightdb] def init(collection: Collection[D]): IO[Unit] = {
    this.collection = collection
    for {
      _ <- collection.db.storeManager[D](collection.name).map(store => this.store = store)
      _ <- listener()
        .map(_.init(collection))
        .ioSeq
        .map(_ => _initialized.set(true))
    } yield ()
  }

  def id(value: String = Unique()): Id[D] = Id(value)

  def apply(id: Id[D])(implicit transaction: Transaction[D]): IO[D] = store(id)

  def get(id: Id[D])(implicit transaction: Transaction[D]): IO[Option[D]] = store.get(id)

  final def set(doc: D)(implicit transaction: Transaction[D]): IO[Option[D]] = {
    recurseOption(doc, (l, d) => l.preSet(d, transaction)).flatMap {
      case Some(d) => for {
        _ <- store.set(d)
        _ <- listener().map(l => l.postSet(d, transaction)).ioSeq
      } yield Some(d)
      case None => IO.pure(None)
    }
  }

  def stream(implicit transaction: Transaction[D]): fs2.Stream[IO, D] = store.stream

  def count(implicit transaction: Transaction[D]): IO[Int] = store.count

  def idStream(implicit transaction: Transaction[D]): fs2.Stream[IO, Id[D]] = store.idStream

  final def delete(doc: D)(implicit transaction: Transaction[D]): IO[Option[D]] = {
    recurseOption(doc, (l, d) => l.preDelete(d, transaction)).flatMap {
      case Some(d) => for {
        _ <- store.delete(d._id)
        _ <- listener().map(l => l.postDelete(d, transaction)).ioSeq
      } yield Some(d)
      case None => IO.pure(None)
    }
  }

  def truncate()(implicit transaction: Transaction[D]): IO[Unit] = listener()
    .map(l => l.truncate(transaction))
    .ioSeq
    .map(_ => ())

  def dispose(): IO[Unit] = listener()
    .map(l => l.dispose())
    .ioSeq
    .map(_ => ())
}