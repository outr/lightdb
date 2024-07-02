package lightdb

import scala.language.implicitConversions

trait DocModel[Doc] {
  private var _fields = List.empty[Field[Doc, _]]

  def fields: List[Field[Doc, _]] = _fields

  object field {
    private def add[V, F <: Field[Doc, V]](field: F): F = synchronized {
      _fields = _fields ::: List(field)
      field
    }

    def apply[V](name: String, get: Doc => V): Field.Basic[Doc, V] =
      add[V, Field.Basic[Doc, V]](Field.Basic(name, get))

    def index[V](name: String, get: Doc => V): Field.Index[Doc, V] =
      add[V, Field.Index[Doc, V]](Field.Index(name, get))

    def unique[V](name: String, get: Doc => V): Field.Unique[Doc, V] =
      add[V, Field.Unique[Doc, V]](Field.Unique(name, get))
  }
}