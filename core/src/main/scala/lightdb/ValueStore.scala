package lightdb

import cats.effect.IO
import fabric.rw.RW
import lightdb.model.{AbstractCollection, DocumentAction}

case class ValueStore[V, D <: Document[D]](key: String,
                                           createV: D => V,
                                           loadStore: () => Store,
                                           collection: AbstractCollection[D],
                                           cached: Boolean = false,
                                           distinct: Boolean = true)
                                          (implicit rw: RW[V]) {
  private lazy val stored = collection.db.stored[List[V]](
    key = s"${collection.collectionName}.valueStore.$key",
    default = Nil,
    cache = cached
  )

  collection.postSet.add((_: DocumentAction, doc: D, _: AbstractCollection[D]) => {
    stored.modify { list => IO {
      val v = createV(doc)
      var l = v :: list
      if (distinct) l = l.distinct
      l
    }}.map(_ => Some(doc))
  })
  collection.postDelete.add((_: DocumentAction, doc: D, _: AbstractCollection[D]) => {
    stored.modify { list => IO {
      val v = createV(doc)
      list.filterNot(_ == v)
    }}.map(_ => Some(doc))
  })
  collection.truncateActions += stored.clear()
}
