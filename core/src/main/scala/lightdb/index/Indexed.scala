package lightdb.index

import fabric.rw.RW
import lightdb.Id
import lightdb.document.Document

trait Indexed[D <: Document[D]] {
  private var _indexes = List.empty[Index[_, D]]

  def indexes: List[Index[_, D]] = _indexes

  val _id: Index[Id[D], D] = index.one("_id", _._id, store = true)

  object index {
    def apply[F](name: String,
                 get: D => List[F],
                 store: Boolean = false,
                 sorted: Boolean = false,
                 tokenized: Boolean = false)
                (implicit rw: RW[F]): Index[F, D] = {
      val index = Index[F, D](
        name = name,
        get = get,
        store = store,
        sorted = sorted,
        tokenized = tokenized
      )
      synchronized {
        _indexes = _indexes ::: List(index)
      }
      index
    }

    def opt[F](name: String,
               get: D => Option[F],
               store: Boolean = false,
               sorted: Boolean = false,
               tokenized: Boolean = false)
              (implicit rw: RW[F]): Index[F, D] = apply[F](name, doc => get(doc).toList, store, sorted, tokenized)

    def one[F](name: String,
               get: D => F,
               store: Boolean = false,
               sorted: Boolean = false,
               tokenized: Boolean = false)
              (implicit rw: RW[F]): Index[F, D] = apply[F](name, doc => List(get(doc)), store, sorted, tokenized)
  }
}