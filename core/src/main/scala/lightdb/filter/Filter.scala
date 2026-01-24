package lightdb.filter

import fabric.*
import fabric.rw.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.spatial.{Geo, Point}
import lightdb.id.Id
import rapid.Task

sealed trait Filter[Doc <: Document[Doc]] {
  def fieldNames: List[String]

  def fields(model: DocumentModel[Doc]): List[Field[Doc, _]] =
    fieldNames.map(model.fieldByName)
}

object Filter {
  def and[Doc <: Document[Doc]](filters: Filter[Doc]*): Filter[Doc] = filters.tail
    .foldLeft(filters.head)((combined, filter) => combined && filter)

  case class Equals[Doc <: Document[Doc], F](fieldName: String, value: F) extends Filter[Doc] {
    def getJson(model: DocumentModel[Doc]): Json = model.fieldByName[F](fieldName).rw.read(value)
    def field(model: DocumentModel[Doc]): Field[Doc, F] = model.fieldByName(fieldName)

    override lazy val fieldNames: List[String] = List(fieldName)
  }
  object Equals {
    def apply[Doc <: Document[Doc], F](field: Field[Doc, F], value: F): Equals[Doc, F] = Equals(field.name, value)
  }

  case class NotEquals[Doc <: Document[Doc], F](fieldName: String, value: F) extends Filter[Doc] {
    def getJson(model: DocumentModel[Doc]): Json = model.fieldByName[F](fieldName).rw.read(value)
    def field(model: DocumentModel[Doc]): Field[Doc, F] = model.fieldByName(fieldName)

    override lazy val fieldNames: List[String] = List(fieldName)
  }
  object NotEquals {
    def apply[Doc <: Document[Doc], F](field: Field[Doc, F], value: F): NotEquals[Doc, F] = NotEquals(field.name, value)
  }

  case class Regex[Doc <: Document[Doc], F](fieldName: String, expression: String) extends Filter[Doc] {
    def getJson(model: DocumentModel[Doc]): Json = Str(expression)
    def field(model: DocumentModel[Doc]): Field[Doc, F] = model.fieldByName(fieldName)

    override lazy val fieldNames: List[String] = List(fieldName)
  }
  object Regex {
    def apply[Doc <: Document[Doc], F](field: Field[Doc, F], expression: String): Regex[Doc, F] = Regex(field.name, expression)
  }

  case class In[Doc <: Document[Doc], F](fieldName: String, values: Seq[F]) extends Filter[Doc] {
    def getJson(model: DocumentModel[Doc]): List[Json] = values.toList.map(model.fieldByName[F](fieldName).rw.read)
    def field(model: DocumentModel[Doc]): Field[Doc, F] = model.fieldByName(fieldName)

    override lazy val fieldNames: List[String] = List(fieldName)
  }
  object In {
    def apply[Doc <: Document[Doc], F](field: Field[Doc, F], values: Seq[F]): In[Doc, F] = In(field.name, values)
  }

  case class RangeLong[Doc <: Document[Doc]](fieldName: String, from: Option[Long], to: Option[Long]) extends Filter[Doc] {
    def field(model: DocumentModel[Doc]): Field[Doc, Long] = model.fieldByName(fieldName)
    override lazy val fieldNames: List[String] = List(fieldName)
  }

  case class RangeDouble[Doc <: Document[Doc]](fieldName: String, from: Option[Double], to: Option[Double]) extends Filter[Doc] {
    def field(model: DocumentModel[Doc]): Field[Doc, Double] = model.fieldByName(fieldName)
    override lazy val fieldNames: List[String] = List(fieldName)
  }

  case class StartsWith[Doc <: Document[Doc], F](fieldName: String, query: String) extends Filter[Doc] {
    def field(model: DocumentModel[Doc]): Field[Doc, F] = model.fieldByName(fieldName)
    override lazy val fieldNames: List[String] = List(fieldName)
  }

  case class EndsWith[Doc <: Document[Doc], F](fieldName: String, query: String) extends Filter[Doc] {
    def field(model: DocumentModel[Doc]): Field[Doc, F] = model.fieldByName(fieldName)
    override lazy val fieldNames: List[String] = List(fieldName)
  }

  case class Contains[Doc <: Document[Doc], F](fieldName: String, query: String) extends Filter[Doc] {
    def field(model: DocumentModel[Doc]): Field[Doc, F] = model.fieldByName(fieldName)
    override lazy val fieldNames: List[String] = List(fieldName)
  }

  case class Exact[Doc <: Document[Doc], F](fieldName: String, query: String) extends Filter[Doc] {
    def field(model: DocumentModel[Doc]): Field[Doc, F] = model.fieldByName(fieldName)
    override lazy val fieldNames: List[String] = List(fieldName)
  }

  case class Distance[Doc <: Document[Doc]](fieldName: String, from: Point, radius: lightdb.distance.Distance) extends Filter[Doc] {
    def field(model: DocumentModel[Doc]): Field[Doc, Geo] = model.fieldByName(fieldName)
    override lazy val fieldNames: List[String] = List(fieldName)
  }

  case class Multi[Doc <: Document[Doc]](minShould: Int, filters: List[FilterClause[Doc]] = Nil) extends Filter[Doc] {
    def conditional(filter: Filter[Doc], condition: Condition, boost: Option[Double] = None): Multi[Doc] =
      copy(filters = filters ::: List(FilterClause(filter, condition, boost)))

    override def fieldNames: List[String] = filters.flatMap(_.filter.fieldNames)
  }

  case class DrillDownFacetFilter[Doc <: Document[Doc]](fieldName: String, path: List[String], showOnlyThisLevel: Boolean = false) extends Filter[Doc] {
    override lazy val fieldNames: List[String] = List(fieldName)

    /**
     * Only returns facets that represent this as the lowest level. If there's another level below this, it will be
     * excluded from the result set.
     */
    lazy val onlyThisLevel: DrillDownFacetFilter[Doc] = copy(showOnlyThisLevel = true)
  }

  /**
   * Parent-side filter that matches when at least one child satisfies the provided child filter.
   */
  trait ExistsChild[Parent <: Document[Parent]] extends Filter[Parent] {
    type Child <: Document[Child]
    type ChildModel <: DocumentModel[Child]

    val relation: ParentChildRelation.Aux[Parent, Child, ChildModel]
    val childFilter: ChildModel => Filter[Child]

    // Resolved into a parent id filter during planning; no direct parent fields referenced here.
    override val fieldNames: List[String] = Nil

    /**
     * Backwards-compatible helper for callers that previously invoked resolution here.
     *
     * ExistsChild is now treated as a pure logical operator; resolution/materialization is owned by FilterPlanner
     * (via ExistsChildResolver).
     */
    final def resolve(parentModel: DocumentModel[Parent]): Task[Filter[Parent]] = {
      FilterPlanner.DefaultExistsChildResolver.resolve(this, parentModel, joinMetadataProvider = None)
    }
  }

  object ExistsChild {
    def apply[
      Parent <: Document[Parent],
      Child0 <: Document[Child0],
      ChildModel0 <: DocumentModel[Child0]
    ](
      relation0: ParentChildRelation.Aux[Parent, Child0, ChildModel0],
      childFilter0: ChildModel0 => Filter[Child0]
    ): ExistsChild[Parent] = new ExistsChild[Parent] {
      override type Child = Child0
      override type ChildModel = ChildModel0
      override val relation: ParentChildRelation.Aux[Parent, Child0, ChildModel0] = relation0
      override val childFilter: ChildModel0 => Filter[Child0] = childFilter0
    }
  }

  sealed trait ChildSemantics
  object ChildSemantics {
    /**
     * Same-child semantics: all child constraints must be satisfied by a single child document.
     */
    case object SameChildAll extends ChildSemantics

    /**
     * Collective semantics: each constraint must be satisfied by at least one child (not necessarily the same child).
     */
    case object CollectiveAll extends ChildSemantics
  }

  /**
   * An explicit, introspectable parent/child filter shape describing "same-child" vs "collective" semantics.
   *
   * - For non-native backends, planners can expand this into `ExistsChild` filters and resolve normally.
   * - For native backends, compilers can pattern match on `semantics` directly.
   */
  trait ChildConstraints[Parent <: Document[Parent]] extends Filter[Parent] {
    type Child <: Document[Child]
    type ChildModel <: DocumentModel[Child]

    val relation: ParentChildRelation.Aux[Parent, Child, ChildModel]
    val semantics: ChildSemantics
    val builds: List[ChildModel => Filter[Child]]

    override val fieldNames: List[String] = Nil

    final def expandToExistsChildFilters: Filter[Parent] = {
      semantics match {
        case ChildSemantics.SameChildAll =>
          val childFilter: ChildModel => Filter[Child] = { cm =>
            if builds.isEmpty then {
              // "exists any child"
              Filter.Multi[Child](minShould = 0)
            } else {
              Filter.and(builds.map(_(cm)): _*)
            }
          }
          Filter.ExistsChild(relation, childFilter)
        case ChildSemantics.CollectiveAll =>
          if builds.isEmpty then {
            Filter.Multi[Parent](minShould = 0)
          } else {
            Filter.and(builds.map(b => Filter.ExistsChild(relation, b)): _*)
          }
      }
    }
  }

  object ChildConstraints {
    def apply[
      Parent <: Document[Parent],
      Child0 <: Document[Child0],
      ChildModel0 <: DocumentModel[Child0]
    ](
      relation0: ParentChildRelation.Aux[Parent, Child0, ChildModel0],
      semantics0: ChildSemantics,
      builds0: List[ChildModel0 => Filter[Child0]]
    ): ChildConstraints[Parent] = new ChildConstraints[Parent] {
      override type Child = Child0
      override type ChildModel = ChildModel0
      override val relation: ParentChildRelation.Aux[Parent, Child0, ChildModel0] = relation0
      override val semantics: ChildSemantics = semantics0
      override val builds: List[ChildModel0 => Filter[Child0]] = builds0
    }
  }

  /**
   * A filter that intentionally matches no documents.
   */
  case class MatchNone[Doc <: Document[Doc]]() extends Filter[Doc] {
    override val fieldNames: List[String] = Nil
  }
}