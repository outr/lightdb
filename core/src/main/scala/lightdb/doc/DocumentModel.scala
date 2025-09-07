package lightdb.doc

import fabric.rw._
import lightdb.CompositeIndex
import lightdb.facet.{FacetConfig, FacetValue}
import lightdb.field.Field._
import lightdb.field.{Field, FieldGetter}
import lightdb.filter.FilterBuilder
import lightdb.id.{Id, StringId}
import lightdb.store.Store
import rapid.{Task, Unique}
import sourcecode.Name

import scala.language.experimental.macros
import scala.language.implicitConversions

trait DocumentModel[Doc <: Document[Doc]] { model =>
  implicit def rw: RW[Doc]

  private var _fields = List.empty[Field[Doc, _]]
  private var _compositeIndexes = List.empty[CompositeIndex[Doc]]

  val _id: UniqueIndex[Doc, Id[Doc]] = field.unique("_id", (doc: Doc) => doc._id)

  def id(value: String = Unique.sync()): Id[Doc] = StringId(value)

  private var _initialized = Set.empty[Store[_, _]]

  final def initialize[Model <: DocumentModel[Doc]](store: Store[Doc, Model]): Task[Unit] = Task.defer {
    val b = synchronized {
      if (_initialized.contains(store)) {
        false
      } else {
        _initialized += store
        true
      }
    }
    if (b) {
      init(store)
    } else {
      Task.unit
    }
  }

  protected def init[Model <: DocumentModel[Doc]](store: Store[Doc, Model]): Task[Unit] = Task.unit

  type F[V] = Field[Doc, V]
  type I[V] = Indexed[Doc, V]
  type U[V] = UniqueIndex[Doc, V]
  type T = Tokenized[Doc]
  type FF = FacetField[Doc]

  def map2Doc(map: Map[String, Any]): Doc

  def fields: List[Field[Doc, _]] = _fields

  def compositeIndexes: List[CompositeIndex[Doc]] = _compositeIndexes

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

    def apply[V: RW](get: Doc => V)(implicit name: Name): Field[Doc, V] = apply(name.value, get)

    def index[V: RW](name: String, get: FieldGetter[Doc, V]): Indexed[Doc, V] = index[V](name, get, stored = true)
    def index[V: RW](name: String, get: FieldGetter[Doc, V], stored: Boolean): Indexed[Doc, V] =
      add[V, Indexed[Doc, V]](Field.indexed(name, get, stored))

    def index[V: RW](name: String, get: Doc => V): Indexed[Doc, V] = index[V](name, get, stored = true)
    def index[V: RW](name: String, get: Doc => V, stored: Boolean): Indexed[Doc, V] =
      add[V, Indexed[Doc, V]](Field.indexed(name, FieldGetter.func(get), stored))

    def index[V: RW](get: Doc => V)(implicit name: Name): Indexed[Doc, V] = index[V](get, stored = true)
    def index[V: RW](get: Doc => V, stored: Boolean)(implicit name: Name): Indexed[Doc, V] = index(name.value, get)

    def indexComposite(fields: List[Field[Doc, _]],
                       include: List[Field[Doc, _]] = Nil)
                      (implicit name: Name): CompositeIndex[Doc] = synchronized {
      val index = CompositeIndex(
        name = name.value,
        model = model,
        fields = fields,
        include = include
      )
      _compositeIndexes = _compositeIndexes ::: List(index)
      index
    }

    def unique[V: RW](name: String, get: FieldGetter[Doc, V]): UniqueIndex[Doc, V] =
      add[V, UniqueIndex[Doc, V]](Field.unique(name, get))

    def unique[V: RW](name: String, get: Doc => V): UniqueIndex[Doc, V] =
      add[V, UniqueIndex[Doc, V]](Field.unique(name, FieldGetter.func(get)))

    def unique[V: RW](get: Doc => V)(implicit name: Name): UniqueIndex[Doc, V] = unique(name.value, get)

    def tokenized(name: String, get: FieldGetter[Doc, String]): Tokenized[Doc] =
      add[String, Tokenized[Doc]](Field.tokenized(name, get))

    def tokenized(name: String, get: Doc => String): Tokenized[Doc] =
      add[String, Tokenized[Doc]](Field.tokenized(name, FieldGetter.func(get)))

    def tokenized(get: Doc => String)(implicit name: Name): Tokenized[Doc] = tokenized(name.value, get)

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