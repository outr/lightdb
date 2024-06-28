package lightdb.filter

import fabric._
import fabric.define.DefType
import fabric.rw._
import lightdb.aggregate.{AggregateFunction, AggregateType}
import lightdb.document.Document
import lightdb.index.Index
import lightdb.index.Index.string2Json

trait AggregateSupport[F, D <: Document[D]] {
  this: Index[F, D] =>

  lazy val max: AggregateFunction[F, F, D] = AggregateFunction(s"${name}Max", this, AggregateType.Max)
  lazy val min: AggregateFunction[F, F, D] = AggregateFunction(s"${name}Min", this, AggregateType.Min)
  lazy val avg: AggregateFunction[Double, F, D] = AggregateFunction(s"${name}Avg", this, AggregateType.Avg)
  lazy val sum: AggregateFunction[F, F, D] = AggregateFunction(s"${name}Sum", this, AggregateType.Sum)
  lazy val count: AggregateFunction[Int, F, D] = AggregateFunction(s"${name}Count", this, AggregateType.Count)
  lazy val countDistinct: AggregateFunction[Int, F, D] = AggregateFunction(s"${name}CountDistinct", this, AggregateType.CountDistinct)
  lazy val group: AggregateFunction[F, F, D] = AggregateFunction(s"${name}Group", this, AggregateType.Group)
  lazy val concat: AggregateFunction[List[F], F, D] = AggregateFunction(s"${name}Concat", this, AggregateType.Concat)(AggregateSupport.ConcatRW)
  lazy val concatDistinct: AggregateFunction[List[F], F, D] = AggregateFunction(s"${name}ConcatDistinct", this, AggregateType.ConcatDistinct)(AggregateSupport.ConcatRW)
}

object AggregateSupport {
  private def ConcatRW[F](implicit fRW: RW[F]): RW[List[F]] = RW.from[List[F]](
    r = list => arr(list.map(fRW.read)),
    w = _.asString.split(";;").toList.map { s =>
      val json = string2Json(s, fRW.definition)
      json.as[F]
    },
    d = DefType.Json
  )
}