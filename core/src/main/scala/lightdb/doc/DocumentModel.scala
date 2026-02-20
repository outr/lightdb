package lightdb.doc

import fabric.rw.*
import lightdb.CompositeIndex
import lightdb.facet.{FacetConfig, FacetValue}
import lightdb.field.Field.*
import lightdb.field.{Field, FieldGetter}
import lightdb.filter.{Filter, FilterBuilder, NestedPathField, ParentChildRelation}
import lightdb.id.{Id, StringId}
import lightdb.store.{Collection, Store}
import rapid.{Task, Unique}
import sourcecode.Name

import scala.language.experimental.macros
import scala.language.implicitConversions

trait DocumentModel[Doc <: Document[Doc]] { model =>
  implicit def rw: RW[Doc]

  lazy val modelName: String = getClass.getSimpleName.replace("$", "")

  private var _fields = List.empty[Field[Doc, _]]
  private var _compositeIndexes = List.empty[CompositeIndex[Doc]]
  private var _nestedPaths = Set.empty[String]

  val _id: UniqueIndex[Doc, Id[Doc]] = field.unique("_id", (doc: Doc) => doc._id)

  def id(value: String = Unique.sync()): Id[Doc] = StringId(value)

  private var _initialized = Set.empty[Store[_, _]]

  final def initialize[Model <: DocumentModel[Doc]](store: Store[Doc, Model]): Task[Unit] = Task.defer {
    val b = synchronized {
      if _initialized.contains(store) then {
        false
      } else {
        _initialized += store
        true
      }
    }
    if b then {
      init(store)
    } else {
      Task.unit
    }
  }

  protected def init[Model <: DocumentModel[Doc]](store: Store[Doc, Model]): Task[Unit] = Task.unit

  type F[V] = Field[Doc, V]
  type I[V] = Indexed[Doc, V]
  type NP[V] = NestedPathField[Doc, V]
  type N[A] = Field.NestedIndex[Doc, List[NestedElemOf[A]#Elem]] { type Access = A }
  type U[V] = UniqueIndex[Doc, V]
  type T = Tokenized[Doc]
  type FF = FacetField[Doc]
  trait Nested[V]
  trait NestedElemOf[A] {
    type Elem
    def rw: RW[List[Elem]]
  }
  object NestedElemOf {
    given [A, E](using ev: A <:< Nested[List[E]], rw0: RW[List[E]]): NestedElemOf[A] with
      override type Elem = E
      override val rw: RW[List[E]] = rw0
  }

  def map2Doc(map: Map[String, Any]): Doc

  def fields: List[Field[Doc, _]] = _fields

  def compositeIndexes: List[CompositeIndex[Doc]] = _compositeIndexes

  /**
   * Declared nested paths for this model (for example: "items", "items.owners").
   *
   * Backends that do not have dedicated nested semantics may ignore this metadata.
   */
  def nestedPaths: Set[String] = _nestedPaths

  /**
   * Declares a nested path on this model and returns the normalized value.
   */
  protected def nestedPath(path: String): String = synchronized {
    val normalized = path.trim
    if normalized.isEmpty then throw new IllegalArgumentException("Nested path cannot be empty.")
    _nestedPaths += normalized
    normalized
  }

  /**
   * Builds a nested filter at the specified path.
   *
   * Notes:
   * - `path` must match a nested path declared on the model (typically through `field.index.nested[...]`).
   * - Semantics are same-element (`NestedSemantics.SameElementAll`), so combined inner predicates
   *   must match within the same nested array element.
   */
  def nested(path: String, filter: Filter[Doc]): Filter[Doc] =
    Filter.Nested(path = path, filter = filter)

  /**
   * Builds a nested filter using this model for typed field references.
   *
   * This is a convenience overload for:
   * `nested(path, build(this))`
   */
  def nested(path: String)(build: this.type => Filter[Doc]): Filter[Doc] =
    Filter.Nested(path = path, filter = build(this))

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

    final class NestedIndexBuilder[Nested](path: String, extract: Doc => List[Nested]) {
      def index[V: RW](get: Nested => V)(implicit name: Name): NestedPathField[Doc, V] = {
        new NestedPathField[Doc, V](s"$path.${name.value}")
      }
    }

    object index {
      def apply[V: RW](name: String, get: FieldGetter[Doc, V]): Indexed[Doc, V] = apply[V](name, get, stored = true)
      def apply[V: RW](name: String, get: FieldGetter[Doc, V], stored: Boolean): Indexed[Doc, V] =
        add[V, Indexed[Doc, V]](Field.indexed(name, get, stored))

      def apply[V: RW](name: String, get: Doc => V): Indexed[Doc, V] = apply[V](name, get, stored = true)
      def apply[V: RW](name: String, get: Doc => V, stored: Boolean): Indexed[Doc, V] =
        add[V, Indexed[Doc, V]](Field.indexed(name, FieldGetter.func(get), stored))

      def apply[V: RW](get: Doc => V)(implicit name: Name): Indexed[Doc, V] = apply[V](get, stored = true)
      def apply[V: RW](get: Doc => V, stored: Boolean)(implicit name: Name): Indexed[Doc, V] = apply(name.value, get, stored)

      def apply[V: RW](get: FieldGetter[Doc, V])(implicit name: Name): Indexed[Doc, V] = apply[V](get, stored = true)
      def apply[V: RW](get: FieldGetter[Doc, V], stored: Boolean)(implicit name: Name): Indexed[Doc, V] =
        add[V, Indexed[Doc, V]](Field.indexed(name.value, get, stored))

      /**
       * Declares a nested indexed list field and exposes typed nested accessors.
       *
       * This is the explicit accessor-builder form. The recommended style is the macro-derived trait
       * form shown in the overload below (`nested[A](...)`) because it provides cleaner declarations.
       *
       * Example (explicit access object):
       * {{{
       * val attrs = field.index.nested(_.attrs) { nested =>
       *   new {
       *     val key = nested.index(_.key)
       *     val percent = nested.index(_.percent)
       *   }
       * }
       * }}}
       */
      def nested[Nested: RW, A](get: Doc => List[Nested], indexParent: Boolean)(build: NestedIndexBuilder[Nested] => A)(implicit name: Name): Field.NestedIndex[Doc, List[Nested]] { type Access = A } = {
        val path = nestedPath(name.value)
        val access: A = build(new NestedIndexBuilder[Nested](path, get))
        add[List[Nested], Field.NestedIndex[Doc, List[Nested]] { type Access = A }](
          Field.nestedIndexed(
            name = name.value,
            get = FieldGetter.func(get),
            stored = true,
            path = path,
            access = access,
            indexParentValue = indexParent
          )
        )
      }

      def nested[Nested: RW, A](get: Doc => List[Nested])(build: NestedIndexBuilder[Nested] => A)(implicit name: Name): Field.NestedIndex[Doc, List[Nested]] { type Access = A } =
        nested(get, indexParent = true)(build)

      /**
       * Declares a nested indexed list field using macro-derived accessors from an `Access` trait.
       *
       * This is the recommended nested declaration style.
       *
       * Example:
       * {{{
       * trait Attrs extends Nested[List[Attr]] {
       *   val key: NP[String]
       *   val percent: NP[Double]
       * }
       *
       * val attrs: N[Attrs] = field.index.nested[Attrs](_.attrs)
       *
       * // Query usage:
       * tx.query.filter(_.attrs.nested(a => a.key === "tract-a" && a.percent >= 0.5))
       * }}}
       */
      inline def nested[A](using ne: NestedElemOf[A], name: Name)(get: Doc => List[ne.Elem], indexParent: Boolean = true): N[A] = {
        val path = nestedPath(name.value)
        val access: A = NestedAccessMacros.deriveAccess[A](path)
        val getter: FieldGetter[Doc, List[ne.Elem]] = FieldGetter.func(get)
        add[List[ne.Elem], Field.NestedIndex[Doc, List[ne.Elem]] { type Access = A }](
          Field.nestedIndexed[Doc, List[ne.Elem], A](
            name = name.value,
            get = getter,
            stored = true,
            path = path,
            access = access,
            indexParentValue = indexParent
          )(using ne.rw)
        ).asInstanceOf[N[A]]
      }
    }

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