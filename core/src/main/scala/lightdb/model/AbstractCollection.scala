package lightdb.model

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import io.chrisdavenport.keysemaphore.KeySemaphore
import lightdb.{DocLock, Document, Id, IndexedLinks, LightDB, MaxLinks, Store}

trait AbstractCollection[D <: Document[D]] {
  // Id-level locking
  private lazy val sem = KeySemaphore.of[IO, Id[D]](_ => 1L).unsafeRunSync()

  def model: DocumentModel[D]

  def collectionName: String

  def autoCommit: Boolean

  def atomic: Boolean

  protected[lightdb] def db: LightDB

  protected lazy val store: Store = db.createStoreInternal(collectionName)

  def idStream: fs2.Stream[IO, Id[D]] = store.keyStream

  def stream: fs2.Stream[IO, D] = store.streamJsonDocs[D](model.rw)

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

  def set(doc: D)(implicit existingLock: DocLock[D] = new DocLock.Empty[D]): IO[D] = withLock(doc._id) { _ =>
    for {
      modified <- model.preSet(doc, this)
      json <- model.preSetJson(model.rw.read(doc), this)
      _ <- store.putJson(doc._id, json)
      _ <- model.postSet(doc, this)
    } yield modified
  }

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
            (implicit existingLock: DocLock[D] = new DocLock.Empty[D]): IO[Option[D]] = withLock(id) { implicit lock =>
    for {
      modifiedId <- model.preDelete(id, this)
      deleted <- get(modifiedId).flatMap {
        case Some(d) => store.delete(id).map(_ => Some(d))
        case None => IO.pure(None)
      }
      _ <- deleted match {
        case Some(doc) => model.postDelete(doc, this)
        case None => IO.unit
      }
    } yield deleted
  }

  def truncate(): IO[Unit] = for {
    _ <- store.truncate()
    _ <- model.indexedLinks.map(_.store.truncate()).sequence
  } yield ()

  def get(id: Id[D]): IO[Option[D]] = store.getJsonDoc(id)(model.rw)

  def apply(id: Id[D]): IO[D] = get(id)
    .map(_.getOrElse(throw new RuntimeException(s"$id not found in $collectionName")))

  def size: IO[Int] = store.size

  def commit(): IO[Unit] = store.commit()

  def dispose(): IO[Unit] = IO.unit

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
