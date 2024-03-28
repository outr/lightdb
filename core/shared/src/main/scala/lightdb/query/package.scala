package lightdb

import lightdb.field.Field
import lightdb.query.Filter.GroupedFilter

import scala.language.implicitConversions

package object query {
  implicit def conditionTuple2Filter[D <: Document[D]](tuple: (Filter[D], Condition)): Filter[D] =
    GroupedFilter(0, List(tuple))

  implicit class FieldQueryExtras[D <: Document[D], F](val field: Field[D, F]) extends AnyVal {
    def ===(value: F): Filter[D] = Filter.Equals(field, value)
    def is(value: F): Filter[D] = Filter.Equals(field, value)
    def includes(values: Seq[F]): Filter[D] = Filter.Includes(field, values)
    def excludes(values: Seq[F]): Filter[D] = Filter.Excludes(field, values)
  }

  implicit class FilterExtras[D <: Document[D]](val filter: Filter[D]) extends AnyVal {
    def &&(that: Filter[D]): Filter[D] = Filter.GroupedFilter(0, List(
      filter -> Condition.Must, that -> Condition.Must
    ))
  }
}