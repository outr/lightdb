package lightdb.model

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import fabric.Json
import fabric.rw.RW
import io.chrisdavenport.keysemaphore.KeySemaphore
import lightdb.{CommitMode, DocLock, Document, Id, IndexedLinks, LightDB, MaxLinks, Store}

import java.util.concurrent.atomic.AtomicBoolean

trait AbstractCollection[D <: Document[D]] extends DocumentActionSupport[D] {
  // Id-level locking
  private lazy val sem = KeySemaphore.of[IO, Id[D]](_ => 1L).unsafeRunSync()

  private val _dirty = new AtomicBoolean(false)

  def isDirty: Boolean = _dirty.get()
  protected def flagDirty(): Unit = _dirty.set(true)

  implicit val rw: RW[D]

  def model: DocumentModel[D]

  def collectionName: String

  def defaultCommitMode: CommitMode = CommitMode.Manual

  def atomic: Boolean

  protected[lightdb] def db: LightDB

  protected lazy val store: Store = db.createStoreInternal(collectionName)

  model.initModel(this)

  def idStream: fs2.Stream[IO, Id[D]] = store.keyStream

  def stream: fs2.Stream[IO, D] = store.streamJsonDocs[D](rw)

  def jsonStream: fs2.Stream[IO, Json] = store.streamJson

  def withLock[Return](id: Id[D])(f: DocLock[D] => IO[Return])
                      (implicit existingLock: DocLock[D] = new DocLock.Empty[D]): IO[Return] = {
    if (atomic && existingLock.isInstanceOf[DocLock.Empty[_]]) {
      val lock: DocLock[D] = existingLock match {
        case DocLock.Set(currentId) =>
          assert(currentId == id, s"Different Id used for lock! Existing: $currentId, New: $id")
          existingLock
        case _ => DocLock.Set[D](id)
      }
      val s = sem(id)
      s
        .acquire
        .flatMap(_ => f(lock))
        .guarantee(s.release)
    } else {
      f(existingLock)
    }
  }

  def set(doc: D, commitMode: CommitMode = defaultCommitMode)
         (implicit existingLock: DocLock[D] = new DocLock.Empty[D]): IO[D] = withLock(doc._id) { lock =>
    doSet(
      doc = doc,
      collection = this,
      set = (id, json) => store.putJson(id, json)
    )(lock).flatMap { doc =>
      mayCommit(commitMode).map(_ => doc)
    }
  }

  def setAll(docs: Seq[D], commitMode: CommitMode = defaultCommitMode): IO[Int] = docs.map(set(_, commitMode)).sequence.map(_.size)

  def modify(id: Id[D])
            (f: Option[D] => IO[Option[D]])
            (implicit existingLock: DocLock[D] = new DocLock.Empty[D]): IO[Option[D]] = withLock(id) { implicit lock =>
    get(id).flatMap { option =>
      f(option).flatMap {
        case Some(doc) => set(doc)(lock).map(Some.apply)
        case None => IO.pure(None)
      }
    }
  }

  def delete(id: Id[D], commitMode: CommitMode = defaultCommitMode)
            (implicit existingLock: DocLock[D] = new DocLock.Empty[D]): IO[Id[D]] = withLock(id) { implicit lock =>
    doDelete(
      id = id,
      collection = this,
      get = apply,
      delete = id => store.delete(id)
    )(lock).flatMap { id =>
      mayCommit(commitMode).map(_ => id)
    }
  }

  private def mayCommit(commitMode: CommitMode): IO[Unit] = {
    if (commitMode == CommitMode.Async) {
      flagDirty()
    }
    commit().whenA(commitMode == CommitMode.Auto)
  }

  def truncate(commitMode: CommitMode = defaultCommitMode): IO[Unit] = for {
    _ <- store.truncate()
    _ <- truncateActions.invoke()
    _ <- mayCommit(commitMode)
  } yield ()

  def get(id: Id[D]): IO[Option[D]] = store.getJsonDoc(id)(rw)

  def apply(id: Id[D]): IO[D] = get(id)
    .map(_.getOrElse(throw new RuntimeException(s"$id not found in $collectionName")))

  def size: IO[Int] = store.size

  def update(): IO[Unit] = if (isDirty) {
    commit()
  } else {
    IO.unit
  }

  def commit(): IO[Unit] = {
    _dirty.set(false)
    store.commit().flatMap { _ =>
      commitActions.invoke()
    }
  }

  def reIndex(): IO[Unit] = model.reIndex(this)

  def dispose(): IO[Unit] = store.dispose().flatMap { _ =>
    disposeActions.invoke()
  }

  /**
   * Creates a key/value stored object with a list of links. This can be incredibly efficient for small lists, but much
   * slower for larger sets of data and a standard index would be preferable.
   *
   * @param name the name of the index
   * @param createV creates the value from the document
   * @param createKey creates a unique identifier from the value
   * @param maxLinks determines how to handle maximum number of links
   */
  def indexedLinks[V](name: String,
                      createV: D => V,
                      createKey: V => String,
                      maxLinks: MaxLinks = MaxLinks.OverflowWarn()): IndexedLinks[V, D] = {
    val il = IndexedLinks[V, D](
      name = name,
      createV = createV,
      createKey = createKey,
      loadStore = () => db.createStoreInternal(s"$collectionName.indexed.$name"),
      collection = this,
      maxLinks = maxLinks
    )
    postSet.add((action: DocumentAction, doc: D, collection: AbstractCollection[D]) => {
      il.add(doc).map(_ => Some(doc))
    })
    postDelete.add((action: DocumentAction, doc: D, collection: AbstractCollection[D]) => {
      il.remove(doc).map(_ => Some(doc))
    })
    truncateActions += il.clear()
    il
  }
}

object AbstractCollection {
  def apply[D <: Document[D]](name: String,
                              db: LightDB,
                              model: DocumentModel[D],
                              defaultCommitMode: CommitMode = CommitMode.Manual,
                              atomic: Boolean = true)(implicit docRW: RW[D]): AbstractCollection[D] = {
    val cm = defaultCommitMode
    val at = atomic
    val lightDB = db
    val documentModel = model
    new AbstractCollection[D] {
      override def collectionName: String = name
      override def defaultCommitMode: CommitMode = cm
      override def atomic: Boolean = at
      override protected[lightdb] def db: LightDB = lightDB

      override implicit val rw: RW[D] = docRW
      override def model: DocumentModel[D] = documentModel
    }
  }
}