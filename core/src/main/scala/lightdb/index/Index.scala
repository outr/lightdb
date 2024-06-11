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

  private def un: String = Unique(length = 8, characters = Unique.LettersLower)

  def max(name: String = un): AggregateFunction[F, F, D] = AggregateFunction(name, this, AggregateType.Max)
  def min(name: String = un): AggregateFunction[F, F, D] = AggregateFunction(name, this, AggregateType.Min)
  def avg(name: String = un): AggregateFunction[Double, F, D] = AggregateFunction(name, this, AggregateType.Avg)
  def sum(name: String = un): AggregateFunction[F, F, D] = AggregateFunction(name, this, AggregateType.Sum)
  def count(name: String = un): AggregateFunction[Int, F, D] = AggregateFunction(name, this, AggregateType.Count)
  def countDistinct(name: String = un): AggregateFunction[Int, F, D] = AggregateFunction(name, this, AggregateType.CountDistinct)
  def group(name: String = un): AggregateFunction[F, F, D] = AggregateFunction(name, this, AggregateType.Group)
  def concat(name: String = un): AggregateFunction[List[F], F, D] = AggregateFunction(name, this, AggregateType.Concat)(Index.ConcatRW)

  def aggregateFilterSupport(name: String): FilterSupport[F, D, AggregateFilter[D]]
}

object Index {
  private def ConcatRW[F](implicit fRW: RW[F]): RW[List[F]] = RW.from[List[F]](
    r = list => arr(list.map(fRW.read)),
    w = _.asString.split(',').toList.map { s =>
      val json = fRW.definition match {
        case DefType.Str => str(s)
        case DefType.Int => num(s.toLong)
        case DefType.Dec => num(BigDecimal(s))
        case DefType.Bool => bool(s.toBoolean)
        case d => throw new RuntimeException(s"Unsupported DefType $d ($s)")
      }
      json.as[F]
    },
    d = DefType.Json
  )
}