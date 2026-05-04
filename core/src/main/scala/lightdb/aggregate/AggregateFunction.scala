package lightdb.aggregate

import fabric.rw.*
import lightdb.distance.Distance
import lightdb.doc.Document
import lightdb.field.Field
import lightdb.filter.{Condition, Filter, FilterClause, FilterSupport}
import lightdb.materialized.Materializable
import lightdb.spatial.{Geo, Point}

case class AggregateFunction[T, V, Doc <: Document[Doc]](name: String,
                                                         field: Field[Doc, V],
                                                         `type`: AggregateType,
                                                         /**
                                                          * Functions evaluated *within each outer bucket* produced by this
                                                          * function. Only meaningful when [[`type`]] is [[AggregateType.Group]];
                                                          * backends ignore the value otherwise. Recursive: a sub-aggregate may
                                                          * itself be a `Group` whose sub-aggregates produce a third level.
                                                          *
                                                          * Result shape — each entry in [[subAggregates]] yields one entry in
                                                          * the per-outer-bucket [[lightdb.materialized.MaterializedAggregate.subResults]] map,
                                                          * keyed by [[AggregateFunction.name]].
                                                          */
                                                         subAggregates: List[AggregateFunction[_, _, Doc]] = Nil)
                                       (implicit val tRW: RW[T]) extends FilterSupport[V, Doc, AggregateFilter[Doc]] with Materializable[Doc, V] {
  def rename(name: String): AggregateFunction[T, V, Doc] = copy(name = name)

  /**
   * Attach inner aggregations to be computed within each bucket produced by this `Group`
   * function. Calling this on a non-Group function is allowed (the value is preserved) but
   * backends will ignore it. Multiple calls accumulate.
   */
  def withSubAggregates(funcs: AggregateFunction[_, _, Doc]*): AggregateFunction[T, V, Doc] =
    copy(subAggregates = subAggregates ::: funcs.toList)

  override implicit def rw: RW[V] = field.rw

  override def is(value: V): AggregateFilter[Doc] = AggregateFilter.Equals(name, field, value)

  override def !==(value: V): AggregateFilter[Doc] = AggregateFilter.NotEquals(name, field, value)

  override def regex(expression: String): AggregateFilter[Doc] = AggregateFilter.Regex(name, field, expression)

  override protected def rangeLong(from: Option[Long], to: Option[Long]): AggregateFilter[Doc] =
    AggregateFilter.RangeLong(name, field.asInstanceOf[Field[Doc, Long]], from, to)

  override protected def rangeDouble(from: Option[Double], to: Option[Double]): AggregateFilter[Doc] =
    AggregateFilter.RangeDouble(name, field.asInstanceOf[Field[Doc, Double]], from, to)

  override def in(values: Seq[V]): AggregateFilter[Doc] = AggregateFilter.In(name, field, values)

  override def startsWith(value: String): AggregateFilter[Doc] = AggregateFilter.StartsWith(name, field, value)
  override def endsWith(value: String): AggregateFilter[Doc] = AggregateFilter.EndsWith(name, field, value)
  override def contains(value: String): AggregateFilter[Doc] = AggregateFilter.Contains(name, field, value)
  override def exactly(value: String): AggregateFilter[Doc] = AggregateFilter.Exact(name, field, value)

  override def group(minShould: Int, filters: (AggregateFilter[Doc], Condition)*): AggregateFilter[Doc] = ???

  override def words(s: String, matchStartsWith: Boolean, matchEndsWith: Boolean): AggregateFilter[Doc] = {
    val words = s.split("\\s+").map { w =>
      if matchStartsWith && matchEndsWith then {
        contains(w)
      } else if matchStartsWith then {
        startsWith(w)
      } else if matchEndsWith then {
        endsWith(w)
      } else {
        exactly(w)
      }
    }.toList
    AggregateFilter.Combined(words)
  }

  override def distance(from: Point, radius: Distance): AggregateFilter[Doc] =
    AggregateFilter.Distance(name, this.asInstanceOf[Field[Doc, Point]], from, radius)

  override def spatialContains(geo: Geo): AggregateFilter[Doc] =
    throw new UnsupportedOperationException("SpatialContains is not supported on aggregate functions")

  override def spatialIntersects(geo: Geo): AggregateFilter[Doc] =
    throw new UnsupportedOperationException("SpatialIntersects is not supported on aggregate functions")
}