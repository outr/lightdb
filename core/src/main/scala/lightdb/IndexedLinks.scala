package lightdb

import cats.effect.IO
import lightdb.model.{AbstractCollection, DocumentAction}

/**
 * Creates a key/value stored object with a list of links. This can be incredibly efficient for small lists, but much
 * slower for larger sets of data and a standard index would be preferable.
 *
 * @param name the name of the index
 * @param createV creates the value from the document
 * @param createKey creates a unique identifier from the value
 * @param collection the collection to associate this with
 * @param maxLinks determines how to handle maximum number of links
 */
case class IndexedLinks[V, D <: Document[D]](name: String,
                                             createV: D => V,
                                             createKey: V => String,
                                             collection: AbstractCollection[D],
                                             maxLinks: MaxLinks = MaxLinks.OverflowWarn()) {
  private lazy val store: Store = collection.db.createStoreInternal(s"${collection.collectionName}.indexedLinks.$name")

  collection.postSet.add((_: DocumentAction, doc: D, _: AbstractCollection[D]) => {
    add(doc).map(_ => Some(doc))
  })
  collection.postDelete.add((_: DocumentAction, doc: D, _: AbstractCollection[D]) => {
    remove(doc).map(_ => Some(doc))
  })
  collection.truncateActions += clear()

  protected[lightdb] def add(doc: D): IO[Unit] = {
    val v = createV(doc)
    for {
      existing <- link(v)
      updated = existing match {
        case Some(l) =>
          val links = l.links ::: List(doc._id)
          val count = links.length
          val updatedLinks = maxLinks match {
            case MaxLinks.NoMax => links
            case MaxLinks.OverflowError(max) => if (count > max) {
              throw new RuntimeException(s"Link overflow for $name: ${createKey(v)} ($max)")
            } else {
              links
            }
            case MaxLinks.OverflowWarn(max) =>
              if (count == max) {
                scribe.warn(s"Link overflow for $name: ${createKey(v)} (max: $max)")
              }
              links
            case MaxLinks.OverflowTrim(max) => if (count > max) {
              links.drop(count - max)
            } else {
              links
            }
          }
          l.copy(links = updatedLinks)
        case None =>
          val key = createKey(v)
          val id = Id[IndexedLink[D]](key)
          IndexedLink(_id = id, links = List(doc._id))
      }
      _ <- store.putJsonDoc(updated)
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
        case Some(l) => store.putJsonDoc(l)
        case None => IO.unit
      }
    } yield ()
  }

  protected[lightdb] def link(value: V): IO[Option[IndexedLink[D]]] = {
    val key = createKey(value)
    val id = Id[IndexedLink[D]](key)
    store.getJsonDoc(id)
  }

  def queryIds(value: V): fs2.Stream[IO, Id[D]] = {
    val io = link(value).map {
      case Some(link) => fs2.Stream[IO, Id[D]](link.links: _*)
      case None => fs2.Stream.empty
    }
    fs2.Stream.force[IO, Id[D]](io)
  }

  def query(value: V): fs2.Stream[IO, D] = queryIds(value).evalMap(collection.apply)

  def clear(): IO[Unit] = store.truncate()
}