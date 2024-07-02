package lightdb.doc

import fabric.rw.RW
import lightdb.Field

import scala.language.implicitConversions

trait DocModel[Doc] {
  def map2Doc(map: Map[String, Any]): Doc

  private var _fields = List.empty[Field[Doc, _]]

  def fields: List[Field[Doc, _]] = _fields

  object field {
    private def add[V, F <: Field[Doc, V]](field: F): F = synchronized {
      _fields = _fields ::: List(field)
      field
    }

    def apply[V: RW](name: String, get: Doc => V): Field[Doc, V] = {
      add[V, Field[Doc, V]](Field.Basic(name, get))
    }

    def index[V: RW](name: String, get: Doc => V): Field.Index[Doc, V] =
      add[V, Field.Index[Doc, V]](Field.Index(name, get))

    def unique[V: RW](name: String, get: Doc => V): Field.Unique[Doc, V] =
      add[V, Field.Unique[Doc, V]](Field.Unique(name, get))
  }
}