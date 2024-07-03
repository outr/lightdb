package lightdb.aggregate

import fabric._
import fabric.define.DefType
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
  lazy val concat: AggregateFunction[List[V], V, Doc] = AggregateFunction(s"${name}Concat", this, AggregateType.Concat)(AggregateSupport.ConcatRW)
  lazy val concatDistinct: AggregateFunction[List[V], V, Doc] = AggregateFunction(s"${name}ConcatDistinct", this, AggregateType.ConcatDistinct)(AggregateSupport.ConcatRW)
}

object AggregateSupport {
  private def ConcatRW[F](implicit fRW: RW[F]): RW[List[F]] = RW.from[List[F]](
    r = list => arr(list.map(fRW.read)),
    w = _.asString.split(";;").toList.map { s =>
      val json = Field.string2Json(s, fRW.definition)
      json.as[F]
    },
    d = DefType.Json
  )
}