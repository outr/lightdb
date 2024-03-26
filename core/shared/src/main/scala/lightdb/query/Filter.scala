package lightdb.query

import lightdb.Document
import lightdb.field.Field

sealed trait Filter[D <: Document[D]] {
  def matches(document: D): Boolean
}

sealed trait Condition

object Condition {
  case object Filter extends Condition
  case object Must extends Condition
  case object MustNot extends Condition
  case object Should extends Condition
}

object Filter {
  case class ParsableSearchTerm[D <: Document[D]](query: String, allowLeadingWildcard: Boolean) extends Filter[D] {
    override def matches(document: D): Boolean = throw new UnsupportedOperationException("ParsableSearchTerm not supported")
  }
  case class GroupedFilter[D <: Document[D]](minimumNumberShouldMatch: Int,
                                             filters: List[(Filter[D], Condition)]) extends Filter[D] {
    override def matches(document: D): Boolean = {
      var should = 0
      var fail = 0
      filters.foreach {
        case (filter, condition) =>
          val b = filter.matches(document)
          condition match {
            case Condition.Must | Condition.Filter if !b => fail +=1
            case Condition.MustNot if b => fail += 1
            case Condition.Should if b => should += 1
            case _ => // Ignore others
          }
      }
      fail == 0 && should >= minimumNumberShouldMatch
    }

    override def toString: String = s"grouped(minimumNumberShouldMatch: $minimumNumberShouldMatch, conditionalTerms: ${filters.map(ct => s"${ct._1} -> ${ct._2}").mkString(", ")})"
  }

  case class Equals[D <: Document[D], F](field: Field[D, F], value: F) extends Filter[D] {
    override def matches(document: D): Boolean = field.getter(document) == value
  }
  case class NotEquals[D <: Document[D], F](field: Field[D, F], value: F) extends Filter[D] {
    override def matches(document: D): Boolean = field.getter(document) != value
  }
  case class Includes[D <: Document[D], F](field: Field[D, F], values: Seq[F]) extends Filter[D] {
    override def matches(document: D): Boolean = values.contains(field.getter(document))
  }
  case class Excludes[D <: Document[D], F](field: Field[D, F], values: Seq[F]) extends Filter[D] {
    override def matches(document: D): Boolean = !values.contains(field.getter(document))
  }
}