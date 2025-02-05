package lightdb.doc

import fabric.rw._
import lightdb._
import lightdb.collection.Collection
import lightdb.facet.{FacetConfig, FacetValue}
import lightdb.field.Field._
import lightdb.field.{Field, FieldGetter}
import lightdb.filter.FilterBuilder
import rapid.{Task, Unique}

import scala.language.implicitConversions

trait DocumentModel[Doc <: Document[Doc]] {
  implicit def rw: RW[Doc]

  private var _fields = List.empty[Field[Doc, _]]

  val _id: UniqueIndex[Doc, Id[Doc]] = field.unique("_id", (doc: Doc) => doc._id)

  def id(value: String = Unique.sync()): Id[Doc] = Id(value)

  def init[Model <: DocumentModel[Doc]](collection: Collection[Doc, Model]): Task[Unit] = Task.unit

  type F[V] = Field[Doc, V]
  type I[V] = Indexed[Doc, V]
  type U[V] = UniqueIndex[Doc, V]
  type T = Tokenized[Doc]
  type FF = FacetField[Doc]

  def map2Doc(map: Map[String, Any]): Doc

  def fields: List[Field[Doc, _]] = _fields

  def indexedFields: List[Field[Doc, _]] = fields.filter(_.indexed)

  def facetFields: List[FF] = fields.collect {
    case ff: FF => ff
  }

  def fieldByName[F](name: String): Field[Doc, F] = _fields
    .find(_.name == name)
    .getOrElse(throw new NullPointerException(s"$name field not found"))
    .asInstanceOf[Field[Doc, F]]

  lazy val builder: FilterBuilder[Doc, this.type] = new FilterBuilder(this, 1, Nil)

  object field {
    private def add[V, F <: Field[Doc, V]](field: F): F = synchronized {
      _fields = _fields ::: List(field)
      field
    }

    def apply[V: RW](name: String, get: FieldGetter[Doc, V]): Field[Doc, V] = {
      add[V, Field[Doc, V]](Field(name, get))
    }

    def apply[V: RW](name: String, get: Doc => V): Field[Doc, V] = {
      add[V, Field[Doc, V]](Field(name, FieldGetter.func(get)))
    }

    def index[V: RW](name: String, get: FieldGetter[Doc, V]): Indexed[Doc, V] =
      add[V, Indexed[Doc, V]](Field.indexed(name, get))

    def index[V: RW](name: String, get: Doc => V): Indexed[Doc, V] =
      add[V, Indexed[Doc, V]](Field.indexed(name, FieldGetter.func(get)))

    def unique[V: RW](name: String, get: FieldGetter[Doc, V]): UniqueIndex[Doc, V] =
      add[V, UniqueIndex[Doc, V]](Field.unique(name, get))

    def unique[V: RW](name: String, get: Doc => V): UniqueIndex[Doc, V] =
      add[V, UniqueIndex[Doc, V]](Field.unique(name, FieldGetter.func(get)))

    def tokenized(name: String, get: FieldGetter[Doc, String]): Tokenized[Doc] =
      add[String, Tokenized[Doc]](Field.tokenized(name, get))

    def tokenized(name: String, get: Doc => String): Tokenized[Doc] =
      add[String, Tokenized[Doc]](Field.tokenized(name, FieldGetter.func(get)))

    def facet(name: String,
              get: FieldGetter[Doc, List[FacetValue]],
              config: FacetConfig): FacetField[Doc] =
      add[List[FacetValue], FacetField[Doc]](Field.facet(name, get, config.hierarchical, config.multiValued, config.requireDimCount))

    def facet(name: String,
              get: Doc => List[FacetValue],
              config: FacetConfig): FacetField[Doc] =
      add[List[FacetValue], FacetField[Doc]](Field.facet(name, FieldGetter.func(get), config.hierarchical, config.multiValued, config.requireDimCount))
  }
}