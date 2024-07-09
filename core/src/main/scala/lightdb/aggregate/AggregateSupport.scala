package lightdb.aggregate

import fabric.rw._
import lightdb.Field

trait AggregateSupport[Doc, V] {
  this: Field[Doc, V] =>

  lazy val max: AggregateFunction[V, V, Doc] = AggregateFunction(s"${name}Max", this, AggregateType.Max)
  lazy val min: AggregateFunction[V, V, Doc] = AggregateFunction(s"${name}Min", this, AggregateType.Min)
  lazy val avg: AggregateFunction[Double, V, Doc] = AggregateFunction(s"${name}Avg", this, AggregateType.Avg)
  lazy val sum: AggregateFunction[V, V, Doc] = AggregateFunction(s"${name}Sum", this, AggregateType.Sum)
  lazy val count: AggregateFunction[Int, V, Doc] = AggregateFunction(s"${name}Count", this, AggregateType.Count)
  lazy val countDistinct: AggregateFunction[Int, V, Doc] = AggregateFunction(s"${name}CountDistinct", this, AggregateType.CountDistinct)
  lazy val group: AggregateFunction[V, V, Doc] = AggregateFunction(s"${name}Group", this, AggregateType.Group)
  lazy val concat: AggregateFunction[List[V], V, Doc] = AggregateFunction(s"${name}Concat", this, AggregateType.Concat)
  lazy val concatDistinct: AggregateFunction[List[V], V, Doc] = AggregateFunction(s"${name}ConcatDistinct", this, AggregateType.ConcatDistinct)
}