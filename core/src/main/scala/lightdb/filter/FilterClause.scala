package lightdb.filter

import lightdb.doc.Document

case class FilterClause[Doc <: Document[Doc]](filter: Filter[Doc], condition: Condition, boost: Option[Double])