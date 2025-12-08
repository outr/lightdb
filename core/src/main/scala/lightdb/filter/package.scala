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
      case (b1: Filter.Multi[Doc], b2: Filter.Multi[Doc]) if b1.minShould == b2.minShould =>
        Filter.Multi(minShould = b1.minShould, filters = b1.filters ::: b2.filters)
      case (_, b: Filter.Multi[Doc]) => b.conditional(filter, Condition.Must)
      case (b: Filter.Multi[Doc], _) => b.conditional(that, Condition.Must)
      case _ => Filter.Multi(minShould = 1).conditional(filter, Condition.Must).conditional(that, Condition.Must)
    }

    def ||(that: Filter[Doc]): Filter[Doc] = (filter, that) match {
      case (b1: Filter.Multi[Doc], b2: Filter.Multi[Doc]) if b1.minShould == b2.minShould =>
        Filter.Multi(minShould = b1.minShould, filters = b1.filters ::: b2.filters)
      case (_, b: Filter.Multi[Doc]) => b.conditional(filter, Condition.Should)
      case (b: Filter.Multi[Doc], _) => b.conditional(that, Condition.Should)
      case _ => Filter.Multi(minShould = 1).conditional(filter, Condition.Should).conditional(that, Condition.Should)
    }
  }

  /**
   * Builds a parent-side filter that matches when a related child satisfies the provided child filter.
   */
  def existsChild[
    Parent <: Document[Parent],
    Child <: Document[Child],
    ChildModel <: DocumentModel[Child]
  ](relation: ParentChildRelation[Parent, Child, ChildModel])
   (childFilter: ChildModel => Filter[Child]): Filter[Parent] =
    Filter.ExistsChild(relation, childFilter)
}
