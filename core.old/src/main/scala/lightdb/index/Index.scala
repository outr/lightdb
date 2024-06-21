package lightdb.index

import fabric.rw._
import fabric._
import fabric.define.DefType
import lightdb.{Document, Unique}
import lightdb.aggregate.{AggregateFilter, AggregateFunction, AggregateType}
import lightdb.query.Filter

trait Index[F, D <: Document[D]] extends FilterSupport[F, D, Filter[D]] {
  indexSupport.index.register(this)

  def fieldName: String
  def indexSupport: IndexSupport[D]
  def get: D => List[F]
  def getJson: D => List[Json] = (doc: D) => get(doc).map(_.json)

  lazy val max: AggregateFunction[F, F, D] = AggregateFunction(s"${fieldName}Max", this, AggregateType.Max)
  lazy val min: AggregateFunction[F, F, D] = AggregateFunction(s"${fieldName}Min", this, AggregateType.Min)
  lazy val avg: AggregateFunction[Double, F, D] = AggregateFunction(s"${fieldName}Avg", this, AggregateType.Avg)
  lazy val sum: AggregateFunction[F, F, D] = AggregateFunction(s"${fieldName}Sum", this, AggregateType.Sum)
  lazy val count: AggregateFunction[Int, F, D] = AggregateFunction(s"${fieldName}Count", this, AggregateType.Count)
  lazy val countDistinct: AggregateFunction[Int, F, D] = AggregateFunction(s"${fieldName}CountDistinct", this, AggregateType.CountDistinct)
  lazy val group: AggregateFunction[F, F, D] = AggregateFunction(s"${fieldName}Group", this, AggregateType.Group)
  lazy val concat: AggregateFunction[List[F], F, D] = AggregateFunction(s"${fieldName}Concat", this, AggregateType.Concat)(Index.ConcatRW)
  lazy val concatDistinct: AggregateFunction[List[F], F, D] = AggregateFunction(s"${fieldName}ConcatDistinct", this, AggregateType.ConcatDistinct)(Index.ConcatRW)

  def aggregateFilterSupport(name: String): FilterSupport[F, D, AggregateFilter[D]]
}

object Index {
  private def ConcatRW[F](implicit fRW: RW[F]): RW[List[F]] = RW.from[List[F]](
    r = list => arr(list.map(fRW.read)),
    w = _.asString.split(";;").toList.map { s =>
      val json = string2Json[F](s)(fRW)
      json.as[F]
    },
    d = DefType.Json
  )

  def string2Json[F](s: String)(rw: RW[F]): Json = rw.definition match {
    case DefType.Str => str(s)
    case DefType.Int => num(s.toLong)
    case DefType.Dec => num(BigDecimal(s))
    case DefType.Bool => bool(s.toBoolean)
    case d => throw new RuntimeException(s"Unsupported DefType $d ($s)")
  }
}