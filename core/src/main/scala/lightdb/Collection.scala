package lightdb

import cats.effect.IO
import fabric.rw.RW

class Collection[D <: Document[D]](val collectionName: String, db: LightDB)(implicit val rw: RW[D]) {
  protected lazy val store: Store = db.createStore(collectionName)

  private var _indexedLinks = List.empty[IndexedLinks[_, D]]

  /**
   * Called before set
   */
  protected def preSet(doc: D): IO[D] = IO.pure(doc)

  def set(doc: D): IO[D] = preSet(doc).flatMap(store.putJson(_)(rw))
  def modify(id: Id[D])(f: Option[D] => IO[Option[D]]): IO[Option[D]] = get(id).flatMap { option =>
    f(option).flatMap {
      case Some(doc) => set(doc).map(Some.apply)
      case None => IO.pure(None)
    }
  }
  def delete(id: Id[D]): IO[Unit] = store.delete(id)
  def truncate(): IO[Unit] = store.truncate()

  def get(id: Id[D]): IO[Option[D]] = store.getJson(id)
  def apply(id: Id[D]): IO[D] = get(id)
    .map(_.getOrElse(throw new RuntimeException(s"$id not found in $collectionName")))

  def indexedLinks[V](name: String,
                      createKey: V => String,
                      createV: D => V): IndexedLinks[V, D] = {
    val il = IndexedLinks[V, D](
      createKey = createKey,
      createV = createV,
      store = db.createStore(s"$collectionName.indexed.$name"),
      this
    )
    synchronized {
      _indexedLinks = il :: _indexedLinks
    }
    il
  }

  def dispose(): IO[Unit] = IO.unit
}

case class IndexedLinks[V, D <: Document[D]](createKey: V => String,
                                             createV: D => V,
                                             store: Store,
                                             collection: Collection[D]) {
  protected[lightdb] def add(doc: D): IO[Unit] = {
    val v = createV(doc)
    for {
      existing <- link(v)
      updated = existing match {
        case Some(l) => l.copy(links = l.links ::: List(doc._id))
        case None =>
          val key = createKey(v)
          val id = Id[IndexedLink[D]](key)
          IndexedLink(_id = id, links = List(doc._id))
      }
      _ <- store.putJson(updated)
    } yield ()
  }

  protected[lightdb] def remove(doc: D): IO[Unit] = {
    val v = createV(doc)
    for {
      existing <- link(v)
      updated = existing match {
        case Some(l) =>
          val updatedLinks = l.links.filterNot(_ == doc._id)
          if (updatedLinks.isEmpty) {
            None
          } else {
            Some(l.copy(links = updatedLinks))
          }
        case None => None
      }
      _ <- updated match {
        case Some(l) => store.putJson(l)
        case None => IO.unit
      }
    } yield ()
  }

  protected[lightdb] def link(value: V): IO[Option[IndexedLink[D]]] = {
    val key = createKey(value)
    val id = Id[IndexedLink[D]](key)
    store.getJson(id)
  }

  def query(value: V): fs2.Stream[IO, D] = {
    val io = link(value).map {
      case Some(link) => fs2.Stream[IO, Id[D]](link.links: _*)
        .evalMap(collection.apply)
      case None => fs2.Stream.empty
    }
    fs2.Stream.force[IO, D](io)
  }
}

case class IndexedLink[D <: Document[D]](_id: Id[IndexedLink[D]],
                                         links: List[Id[D]]) extends Document[IndexedLink[D]]

object IndexedLink {
  implicit def rw[D <: Document[D]]: RW[IndexedLink[D]] = RW.gen
}