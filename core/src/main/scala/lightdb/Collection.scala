package lightdb

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.{catsSyntaxApplicativeByName, toTraverseOps}
import fabric.Json
import fabric.rw.RW
import io.chrisdavenport.keysemaphore.KeySemaphore
import lightdb.index._

abstract class Collection[D <: Document[D]](val collectionName: String,
                                            protected[lightdb] val db: LightDB,
                                            val autoCommit: Boolean = false,
                                            val atomic: Boolean = true) {
  type Field[F] = IndexedField[F, D]

  implicit val rw: RW[D]

  protected lazy val store: Store = db.createStoreInternal(collectionName)

  // Id-level locking
  private lazy val sem = KeySemaphore.of[IO, Id[D]](_ => 1L).unsafeRunSync()

  private var _indexedLinks = List.empty[IndexedLinks[_, D]]

  def idStream: fs2.Stream[IO, Id[D]] = store.keyStream

  def stream: fs2.Stream[IO, D] = store.streamJsonDocs[D]

  /**
   * Called before preSetJson and before the data is set to the database
   */
  protected def preSet(doc: D): IO[D] = IO.pure(doc)

  /**
   * Called after preSet and before the data is set to the database
   */
  protected def preSetJson(json: Json): IO[Json] = IO.pure(json)

  /**
   * Called after set
   */
  protected def postSet(doc: D): IO[Unit] = for {
    // Update IndexedLinks
    _ <- _indexedLinks.map(_.add(doc)).sequence
    _ <- commit().whenA(autoCommit)
  } yield ()

  protected def preDelete(id: Id[D]): IO[Id[D]] = IO.pure(id)

  protected def postDelete(doc: D): IO[Unit] = for {
    // Update IndexedLinks
    _ <- _indexedLinks.map(_.remove(doc)).sequence
    _ <- commit().whenA(autoCommit)
  } yield ()

  def set(doc: D)(implicit existingLock: DocLock[D] = new DocLock.Empty[D]): IO[D] = withLock(doc._id) { _ =>
    for {
      modified <- preSet(doc)
      json <- preSetJson(rw.read(doc))
      _ <- store.putJson(doc._id, json)
      _ <- postSet(doc)
    } yield modified
  }

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
      modifiedId <- preDelete(id)
      deleted <- get(modifiedId).flatMap {
        case Some(d) => store.delete(id).map(_ => Some(d))
        case None => IO.pure(None)
      }
      _ <- deleted match {
        case Some(doc) => postDelete(doc)
        case None => IO.unit
      }
    } yield deleted
  }

  def truncate(): IO[Unit] = for {
    _ <- store.truncate()
    _ <- _indexedLinks.map(_.store.truncate()).sequence
  } yield ()

  def get(id: Id[D]): IO[Option[D]] = store.getJsonDoc(id)
  def apply(id: Id[D]): IO[D] = get(id)
    .map(_.getOrElse(throw new RuntimeException(s"$id not found in $collectionName")))

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
      _indexedLinks = il :: _indexedLinks
    }
    il
  }

  def size: IO[Int] = store.size

  def commit(): IO[Unit] = store.commit()

  def dispose(): IO[Unit] = IO.unit
}

object Collection {
  def apply[D <: Document[D]](collectionName: String,
                              db: LightDB,
                              autoCommit: Boolean = false)(implicit docRW: RW[D]): Collection[D] =
    new Collection[D](collectionName, db, autoCommit = autoCommit) {
      override implicit val rw: RW[D] = docRW
    }
}