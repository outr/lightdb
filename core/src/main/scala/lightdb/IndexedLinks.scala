package lightdb

import cats.effect.IO

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

  def queryIds(value: V): fs2.Stream[IO, Id[D]] = {
    val io = link(value).map {
      case Some(link) => fs2.Stream[IO, Id[D]](link.links: _*)
      case None => fs2.Stream.empty
    }
    fs2.Stream.force[IO, Id[D]](io)
  }

  def query(value: V): fs2.Stream[IO, D] = queryIds(value).evalMap(collection.apply)
}
