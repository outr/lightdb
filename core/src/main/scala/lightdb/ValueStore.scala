package lightdb

import cats.effect.IO
import cats.implicits.toTraverseOps
import fabric.rw.RW
import lightdb.model.{AbstractCollection, DocumentAction}

case class ValueStore[V, D <: Document[D]](key: String,
                                           createV: D => List[V],
                                           collection: AbstractCollection[D],
                                           includeIds: Boolean = false,
                                           persistence: Persistence = Persistence.Stored)
                                          (implicit rw: RW[V]) {
  private implicit val facetRW: RW[Facet] = RW.gen

  private lazy val stored = collection.db.stored[Map[V, Facet]](
    key = s"${collection.collectionName}.valueStore.$key",
    default = Map.empty,
    persistence = persistence
  )

  def facets: IO[Map[V, Facet]] = stored.get()

  def facet(v: V): IO[Facet] = facets.map(_.getOrElse(v, Facet()))

  def values: IO[Set[V]] = facets.map(_.keySet)

  collection.postSet.add((_: DocumentAction, doc: D, _: AbstractCollection[D]) => {
    createV(doc).map { v =>
      stored.modify { map =>
        IO {
          var facet = map.getOrElse(v, Facet())
          var ids = facet.ids
          if (includeIds) {
            ids = ids + doc._id
          } else {
            ids = Set.empty
          }
          facet = facet.copy(facet.count + 1, ids)
          map + (v -> facet)
        }
      }
    }.sequence.map(_ => Some(doc))
  })
  collection.postDelete.add((_: DocumentAction, doc: D, _: AbstractCollection[D]) => {
    createV(doc).map { v =>
      stored.modify { map =>
        IO {
          map - v
        }
      }
    }.sequence.map(_ => Some(doc))
  })
  collection.truncateActions += stored.clear()

  case class Facet(count: Int = 0, ids: Set[Id[D]] = Set.empty[Id[D]])
}
