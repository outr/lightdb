package lightdb.collection

import cats.effect.IO
import lightdb.{Id, LightDB}
import lightdb.document.{Document, DocumentListener, DocumentModel}
import lightdb.transaction.Transaction
import lightdb.util.Initializable
import cats.implicits._
import fabric.rw.RW

case class Collection[D <: Document[D], M <: DocumentModel[D]](name: String,
                                        model: M,
                                        db: LightDB)
                                       (implicit rw: RW[D]) extends Initializable { collection =>
  private implicit class ListIO[R](list: List[IO[R]]) {
    def ioSeq: IO[Unit] = if (model.parallel) {
      list.parSequence.map(_ => ())
    } else {
      list.sequence.map(_ => ())
    }
  }

  private def recurseOption(doc: D,
                            invoke: (DocumentListener[D], D) => IO[Option[D]],
                            listeners: List[DocumentListener[D]] = model.listener()): IO[Option[D]] = listeners.headOption match {
    case Some(l) => invoke(l, doc).flatMap {
      case Some(v) => recurseOption(v, invoke, listeners.tail)
      case None => IO.pure(None)
    }
    case None => IO.pure(Some(doc))
  }

  override protected def initialize(): IO[Unit] = for {
    _ <- IO(model.collection = this)
    _ <- db.storeManager[D](name).map(store => model.store = store)
    _ <- model.listener().map(_.init(this)).ioSeq.map(_ => model._initialized.set(true))
  } yield ()

  object transaction {
    def apply[Return](f: Transaction[D] => IO[Return]): IO[Return] = create()
      .flatMap { transaction =>
        f(transaction).guarantee(release(transaction))
      }

    private def create(): IO[Transaction[D]] = for {
      transaction <- IO(Transaction[D](collection))
      _ <- model.listener().map(l => l.transactionStart(transaction)).ioSeq
    } yield transaction

    private def release(transaction: Transaction[D]): IO[Unit] = for {
      _ <- transaction.commit()
      _ <- model.listener().map(l => l.transactionEnd(transaction)).ioSeq
    } yield ()
  }

  def apply(id: Id[D])(implicit transaction: Transaction[D]): IO[D] = model.store(id)

  def get(id: Id[D])(implicit transaction: Transaction[D]): IO[Option[D]] = model.store.get(id)

  final def set(doc: D)(implicit transaction: Transaction[D]): IO[Option[D]] = {
    recurseOption(doc, (l, d) => l.preSet(d, transaction)).flatMap {
      case Some(d) => for {
        _ <- model.store.set(d)
        _ <- model.listener().map(l => l.postSet(d, transaction)).ioSeq
      } yield Some(d)
      case None => IO.pure(None)
    }
  }

  def set(stream: fs2.Stream[IO, D])(implicit transaction: Transaction[D]): IO[Int] = stream
    .evalMap(set)
    .compile
    .count
    .map(_.toInt)

  def set(docs: Seq[D])(implicit transaction: Transaction[D]): IO[Int] = set(fs2.Stream(docs: _*))

  def modify(id: Id[D], lock: Boolean = true)
            (f: Option[D] => IO[Option[D]])
            (implicit transaction: Transaction[D]): IO[Option[D]] = transaction.mayLock(id, lock) {
    get(id).flatMap { option =>
      f(option).flatMap {
        case Some(doc) => set(doc)
        case None => IO.pure(None)
      }
    }
  }

  def stream(implicit transaction: Transaction[D]): fs2.Stream[IO, D] = model.store.stream

  def count(implicit transaction: Transaction[D]): IO[Int] = model.store.count

  def idStream(implicit transaction: Transaction[D]): fs2.Stream[IO, Id[D]] = model.store.idStream

  private[lightdb] def commit(transaction: Transaction[D]): IO[Unit] = model.listener()
    .map(l => l.commit(transaction))
    .ioSeq

  private[lightdb] def rollback(transaction: Transaction[D]): IO[Unit] = model.listener()
    .map(l => l.rollback(transaction))
    .ioSeq

  final def delete(doc: D)(implicit transaction: Transaction[D]): IO[Option[D]] = {
    recurseOption(doc, (l, d) => l.preDelete(d, transaction)).flatMap {
      case Some(d) => model.store.delete(d._id).flatMap {
        case true => model
          .listener()
          .map(l => l.postDelete(d, transaction))
          .ioSeq
          .map(_ => Some(d))
        case false => IO.pure(None)
      }
      case None => IO.pure(None)
    }
  }

  def delete(stream: fs2.Stream[IO, D])(implicit transaction: Transaction[D]): IO[Int] = stream
    .evalMap(delete)
    .unNone
    .compile
    .count
    .map(_.toInt)

  def delete(docs: Seq[D])(implicit transaction: Transaction[D]): IO[Int] = delete(fs2.Stream(docs: _*))

  def delete(id: Id[D])(implicit transaction: Transaction[D]): IO[Option[D]] = get(id)
    .flatMap {
      case Some(doc) => delete(doc)
      case None => IO.pure(None)
    }

  def truncate()(implicit transaction: Transaction[D]): IO[Int] = for {
    removed <- model.store.truncate()
    _ <- model.listener()
      .map(l => l.truncate(transaction))
      .ioSeq
  } yield removed

  def update(): IO[Unit] = IO.unit

  def dispose(): IO[Unit] = model.listener()
    .map(l => l.dispose())
    .ioSeq
}