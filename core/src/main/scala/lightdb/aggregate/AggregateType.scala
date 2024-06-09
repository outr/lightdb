package lightdb.aggregate

sealed trait AggregateType

object AggregateType {
  case object Max extends AggregateType
  case object Min extends AggregateType
  case object Avg extends AggregateType
  case object Sum extends AggregateType
  case object Count extends AggregateType
  case object Group extends AggregateType
  case object Concat extends AggregateType
}