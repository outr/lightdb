package lightdb.filter

import fabric._
import fabric.rw._
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.spatial.{Geo, Point}
import lightdb.id.Id
import profig.Profig
import rapid.Task

import scala.collection.mutable
import scala.util.{Failure, Success}

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
  case class ExistsChild[
    Parent <: Document[Parent],
    Child <: Document[Child],
    ChildModel <: DocumentModel[Child]
  ](
    relation: ParentChildRelation[Parent, Child, ChildModel],
    childFilter: ChildModel => Filter[Child]
  ) extends Filter[Parent] {
    // Resolved into a parent id filter during planning; no direct parent fields referenced here.
    override val fieldNames: List[String] = Nil

    def resolve(parentModel: DocumentModel[Parent]): Task[Filter[Parent]] = {
      val parentIdField = parentModel._id.name
      relation.childStore.transaction { childTx =>
        val model: ChildModel = childTx.store.model
        val cf = childFilter(model)
        val parentField = relation.parentField(model)
        // Bound parent-id materialization to avoid blowing memory on very large datasets.
        //
        // Default is unbounded to preserve historical behavior. Opt-in with:
        // -Dlightdb.existsChild.maxParentIds=<N>
        // (cap is on DISTINCT parent ids, not raw child rows).
        val maxDistinct: Int = {
          Profig("lightdb.existsChild.maxParentIds")
            .get()
            .getOrElse(Null)
            .as[Option[Int]]
            .filter(_ >= 0)
            .getOrElse(Int.MaxValue)
        }

        final class TooManyParents extends RuntimeException("Too many distinct parent ids")
        val seen = mutable.HashSet.empty[Id[Parent]]

        childTx.query
          .filter(_ => cf)
          .value(_ => parentField)
          .stream
          .evalMap { pid =>
            Task {
              if (pid != null && seen.add(pid) && seen.size > maxDistinct) throw new TooManyParents
            }
          }
          .drain
          .attempt
          .map {
            case Success(_) =>
              if (seen.isEmpty) Filter.MatchNone[Parent]()
              else Filter.In[Parent, Id[Parent]](parentIdField, seen.toSeq)
            case Failure(_: TooManyParents) =>
              throw new IllegalArgumentException(
                s"ExistsChild resolution exceeded maxParentIds=$maxDistinct distinct parent ids. " +
                  s"Add more selective child filters, redesign to avoid broad ExistsChild, or raise " +
                  s"'lightdb.existsChild.maxParentIds'."
              )
            case Failure(t) =>
              throw t
          }
      }
    }
  }

  /**
   * A filter that intentionally matches no documents.
   */
  case class MatchNone[Doc <: Document[Doc]]() extends Filter[Doc] {
    override val fieldNames: List[String] = Nil
  }
}