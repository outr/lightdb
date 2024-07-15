package lightdb.doc

import fabric.rw._
import lightdb.{Field, Id, Unique}

import scala.language.implicitConversions

trait DocumentModel[Doc <: Document[Doc]] {
  implicit def rw: RW[Doc]

  private var _fields = List.empty[Field[Doc, _]]

  val _id: Field.Unique[Doc, Id[Doc]] = field.unique("_id", _._id)

  def id(value: String = Unique()): Id[Doc] = Id(value)

  type F[V] = Field[Doc, V]
  type I[V] = Field.Index[Doc, V]
  type U[V] = Field.Unique[Doc, V]

  def map2Doc(map: Map[String, Any]): Doc

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