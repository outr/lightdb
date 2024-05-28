package lightdb.model

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import fabric.rw.RW
import io.chrisdavenport.keysemaphore.KeySemaphore
import lightdb.{DocLock, Document, Id, IndexedLinks, LightDB, MaxLinks, Store}

trait AbstractCollection[D <: Document[D]] extends DocumentActionSupport[D] {
  // Id-level locking
  private lazy val sem = KeySemaphore.of[IO, Id[D]](_ => 1L).unsafeRunSync()

  implicit val rw: RW[D]

  def model: DocumentModel[D]

  def collectionName: String

  def autoCommit: Boolean

  def atomic: Boolean

  protected[lightdb] def db: LightDB

  protected lazy val store: Store = db.createStoreInternal(collectionName)

  model.initModel(this)

  def idStream: fs2.Stream[IO, Id[D]] = store.keyStream

  def stream: fs2.Stream[IO, D] = store.streamJsonDocs[D](rw)

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

  def set(doc: D)(implicit existingLock: DocLock[D] = new DocLock.Empty[D]): IO[D] = withLock(doc._id) { lock =>
    doSet(
      doc = doc,
      collection = this,
      set = (id, json) => store.putJson(id, json)
    )(lock).flatMap { doc =>
      commit().whenA(autoCommit).map(_ => doc)
    }
  }

  def set(docs: Seq[D]): IO[Int] = docs.map(set).sequence.map(_.size)

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

  def delete(id: Id[D])
            (implicit existingLock: DocLock[D] = new DocLock.Empty[D]): IO[Id[D]] = withLock(id) { implicit lock =>
    doDelete(
      id = id,
      collection = this,
      get = apply,
      delete = id => store.delete(id)
    )(lock).flatMap { id =>
      commit().whenA(autoCommit).map(_ => id)
    }
  }

  def truncate(): IO[Unit] = for {
    _ <- store.truncate()
    _ <- model.indexedLinks.map(_.store.truncate()).sequence
    _ <- truncateActions.invoke()
    _ <- commit().whenA(autoCommit)
  } yield ()

  def get(id: Id[D]): IO[Option[D]] = store.getJsonDoc(id)(rw)

  def apply(id: Id[D]): IO[D] = get(id)
    .map(_.getOrElse(throw new RuntimeException(s"$id not found in $collectionName")))

  def size: IO[Int] = store.size

  def commit(): IO[Unit] = store.commit().flatMap { _ =>
    commitActions.invoke()
  }

  def dispose(): IO[Unit] = store.dispose().flatMap { _ =>
    disposeActions.invoke()
  }

  /**
   * Creates a key/value stored object with a list of links. This can be incredibly efficient for small lists, but much
   * slower for larger sets of data and a standard index would be preferable.
   */
  def indexedLinks[V](name: String,
                      createKey: V => String,
                      createV: D => V,
                      maxLinks: MaxLinks = MaxLinks.OverflowWarn()): IndexedLinks[V, D] = {
    val il = IndexedLinks[V, D](
      name = name,
      createKey = createKey,
      createV = createV,
      loadStore = () => db.createStoreInternal(s"$collectionName.indexed.$name"),
      collection = this,
      maxLinks = maxLinks
    )
    synchronized {
      model._indexedLinks = il :: model._indexedLinks
    }
    il
  }
}

object AbstractCollection {
  def apply[D <: Document[D]](name: String,
                              db: LightDB,
                              model: DocumentModel[D],
                              autoCommit: Boolean = false,
                              atomic: Boolean = true)(implicit docRW: RW[D]): AbstractCollection[D] = {
    val ac = autoCommit
    val at = atomic
    val lightDB = db
    val documentModel = model
    new AbstractCollection[D] {
      override def collectionName: String = name
      override def autoCommit: Boolean = ac
      override def atomic: Boolean = at
      override protected[lightdb] def db: LightDB = lightDB

      override implicit val rw: RW[D] = docRW
      override def model: DocumentModel[D] = documentModel
    }
  }
}