package lightdb

import lightdb.doc.{Document, DocumentModel}

import scala.language.implicitConversions

package object filter {
  implicit class ListFilterExtras[V, Doc <: Document[Doc], Filter](fs: FilterSupport[List[V], Doc, Filter]) {
    def has(value: V): Filter = fs.is(List(value))
    def hasAny(values: List[V]): Filter = {
      fs.group(
        minShould = 1,
        filters = values.map { v =>
          has(v) -> Condition.Should
        }: _*
      )
    }
  }
  implicit class SetFilterExtras[V, Doc, Filter](fs: FilterSupport[Set[V], Doc, Filter]) {
    def has(value: V): Filter = fs.is(Set(value))
  }
  implicit class FilterExtras[Doc <: Document[Doc]](val filter: Filter[Doc]) extends AnyVal {
    def &&(that: Filter[Doc]): Filter[Doc] = (filter, that) match {
      // Flattening two Multis merges their clause lists under ONE
      // minShould. That is only sound when neither side carries a
      // Should clause: merging two Should-groups (e.g. two `anyOf`
      // token/space groups) would let a match in EITHER group satisfy
      // the single minShould, silently turning the AND into an OR.
      // Should-carrying operands nest as a whole Must clause instead —
      // each keeps its own minShould when compiled.
      case (b1: Filter.Multi[Doc], b2: Filter.Multi[Doc])
        if b1.minShould == b2.minShould &&
          !b1.filters.exists(_.condition == Condition.Should) &&
          !b2.filters.exists(_.condition == Condition.Should) =>
        Filter.Multi(minShould = b1.minShould, filters = b1.filters ::: b2.filters)
      case (_, b: Filter.Multi[Doc]) => b.conditional(filter, Condition.Must)
      case (b: Filter.Multi[Doc], _) => b.conditional(that, Condition.Must)
      case _ => Filter.Multi(minShould = 1).conditional(filter, Condition.Must).conditional(that, Condition.Must)
    }

    def ||(that: Filter[Doc]): Filter[Doc] = (filter, that) match {
      // Merging into an alternative group is only sound when the group
      // is PURE Should with minShould = 1 — "any one of these". A Multi
      // carrying Must/MustNot/Filter clauses is a conjunction: pouring
      // the other operand in as a bare Should makes the conjunction's
      // Must clauses required ALONGSIDE the alternative, turning
      // (a && b) || x into a && b && x. Such operands are wrapped
      // whole as Should clauses of a fresh any-of group instead.
      case (b1: Filter.Multi[Doc], b2: Filter.Multi[Doc])
        if b1.minShould == 1 && b2.minShould == 1 &&
          b1.filters.forall(_.condition == Condition.Should) &&
          b2.filters.forall(_.condition == Condition.Should) =>
        Filter.Multi(minShould = 1, filters = b1.filters ::: b2.filters)
      case (_, b: Filter.Multi[Doc])
        if b.minShould == 1 && b.filters.forall(_.condition == Condition.Should) =>
        b.conditional(filter, Condition.Should)
      case (b: Filter.Multi[Doc], _)
        if b.minShould == 1 && b.filters.forall(_.condition == Condition.Should) =>
        b.conditional(that, Condition.Should)
      case _ => Filter.Multi(minShould = 1).conditional(filter, Condition.Should).conditional(that, Condition.Should)
    }
  }
}
