package lightdb.sqlite

import lightdb.Document
import lightdb.query.Filter

case class SQLFilter[D <: Document[D]](sql: String, args: List[Any]) extends Filter[D] with SQLPart