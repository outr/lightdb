package lightdb

import cats.effect.IO
import cats.implicits.toTraverseOps
import fabric.define.DefType
import fabric.{arr, obj}
import fabric.rw._
import lightdb.model.{AbstractCollection, DocumentAction}

import java.util.concurrent.atomic.AtomicBoolean

case class ValueStore[V, D <: Document[D]](key: String,
                                           createV: D => List[V],
                                           collection: AbstractCollection[D],
                                           includeIds: Boolean = false,
                                           persistence: Persistence = Persistence.Stored)
                                          (implicit rw: RW[V]) {
  private val _initialized = new AtomicBoolean(false)

  private lazy val stored = collection.db.stored[Map[V, Facet[D]]](
    key = s"${collection.collectionName}.valueStore.$key",
    default = Map.empty,
    persistence = persistence
  )

  private def init(): IO[Unit] = if (_initialized.compareAndSet(false, true)) {
    if (persistence == Persistence.Memory) {
      collection.stream.foreach(set).compile.drain
    } else {
      IO.unit
    }
  } else {
    IO.unit
  }

  def facets: IO[Map[V, Facet[D]]] = init().flatMap(_ => stored.get())

  def facet(v: V): IO[Facet[D]] = facets.map(_.getOrElse(v, Facet()))

  def values: IO[Set[V]] = facets.map(_.keySet)

  private def set(doc: D): IO[Unit] = {
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
    }.sequence.map(_ => ())
  }

  collection.postSet.add((_: DocumentAction, doc: D, _: AbstractCollection[D]) => {
    init().flatMap { _ =>
      set(doc).map(_ => Some(doc))
    }
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
}

case class Facet[D <: Document[D]](count: Int = 0, ids: Set[Id[D]] = Set.empty[Id[D]])

object Facet {
  implicit def rw[D <: Document[D]]: RW[Facet[D]] = RW.from[Facet[D]](
    r = f => obj("count" -> f.count.json, "ids" -> arr(f.ids.toList.map(_.json): _*)),
    w = j => Facet[D](
      count = j("count").asInt,
      ids = j("ids").asVector.map(_.asString).map(Id.apply[D]).toSet
    ),
    d = DefType.Obj(
      Some("lightdb.Facet"),
      "count" -> DefType.Int,
      "ids" -> DefType.Arr(DefType.Str)
    )
  )
}