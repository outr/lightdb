package lightdb.doc

import fabric.rw._
import lightdb.{Field, Id, Indexed, Tokenized, Unique, UniqueIndex}

import scala.language.implicitConversions

trait DocumentModel[Doc <: Document[Doc]] {
  implicit def rw: RW[Doc]

  private var _fields = List.empty[Field[Doc, _]]

  val _id: UniqueIndex[Doc, Id[Doc]] = field.unique("_id", _._id)

  def id(value: String = Unique()): Id[Doc] = Id(value)

  type F[V] = Field[Doc, V]
  type I[V] = Indexed[Doc, V]
  type U[V] = UniqueIndex[Doc, V]
  type T = Tokenized[Doc]

  def map2Doc(map: Map[String, Any]): Doc

  def fields: List[Field[Doc, _]] = _fields

  object field {
    private def add[V, F <: Field[Doc, V]](field: F): F = synchronized {
      _fields = _fields ::: List(field)
      field
    }

    def apply[V: RW](name: String, get: Doc => V): Field[Doc, V] = {
      add[V, Field[Doc, V]](Field(name, get))
    }

    def index[V: RW](name: String, get: Doc => V): Indexed[Doc, V] =
      add[V, Indexed[Doc, V]](Field.indexed(name, get))

    def unique[V: RW](name: String, get: Doc => V): UniqueIndex[Doc, V] =
      add[V, UniqueIndex[Doc, V]](Field.unique(name, get))

    def tokenized(name: String, get: Doc => String): Tokenized[Doc] =
      add[String, Tokenized[Doc]](Field.tokenized(name, doc => get(doc)))
  }
}