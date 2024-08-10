package lightdb.filter

case class FilterClause[Doc](filter: Filter[Doc], condition: Condition, boost: Option[Double])