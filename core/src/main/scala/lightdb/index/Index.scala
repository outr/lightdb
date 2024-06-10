package lightdb.index

import fabric.rw.{Convertible, RW}
import fabric._
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
  def group(name: String = un): AggregateFunction[F, F, D] = AggregateFunction(name, this, AggregateType.Group)
  def concat(name: String = un): AggregateFunction[List[String], F, D] = AggregateFunction(name, this, AggregateType.Concat)(Index.ConcatRW)

  def aggregateFilterSupport(name: String): FilterSupport[F, D, AggregateFilter[D]]
}

object Index {
  private val ConcatRW: RW[List[String]] = RW.string[List[String]](
    asString = _.mkString(","),
    fromString = _.split(',').toList
  )
}