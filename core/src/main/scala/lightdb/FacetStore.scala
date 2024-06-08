package lightdb

import cats.effect.IO
import fabric.rw.RW
import lightdb.model.{AbstractCollection, DocumentAction}

case class FacetStore[V, D <: Document[D]](key: String,
                                           createV: D => Option[V],
                                           collection: AbstractCollection[D],
                                           cached: Boolean = false,
                                           includeIds: Boolean = false)
                                          (implicit rw: RW[V]) {
  private implicit val facetRW: RW[Facet] = RW.gen

  private lazy val stored = collection.db.stored[Map[V, Facet]](
    key = s"${collection.collectionName}.valueStore.$key",
    default = Map.empty,
    cache = cached
  )

  def facets: IO[Map[V, Facet]] = stored.get()

  def values: IO[Set[V]] = facets.map(_.keySet)

  collection.postSet.add((_: DocumentAction, doc: D, _: AbstractCollection[D]) => {
    (createV(doc) match {
      case Some(v) =>
        scribe.info(s"Modifying $v")
        stored.modify { map => IO {
          var facet = map.getOrElse(v, Facet())
          var ids = facet.ids
          if (includeIds) {
            ids = ids + doc._id
          } else {
            ids = Set.empty
          }
          facet = facet.copy(facet.count + 1, ids)
          scribe.info(s"Finished modifying $v")
          map + (v -> facet)
        }}
      case None => IO.unit
    }).map(_ => Some(doc))
  })
  collection.postDelete.add((_: DocumentAction, doc: D, _: AbstractCollection[D]) => {
    createV(doc) match {
      case Some(v) => stored.modify { map => IO {
        map - v
      }}.map(_ => Some(doc))
      case None => IO.pure(Some(doc))
    }
  })
  collection.truncateActions += stored.clear()

  case class Facet(count: Int = 0, ids: Set[Id[D]] = Set.empty[Id[D]])
}
