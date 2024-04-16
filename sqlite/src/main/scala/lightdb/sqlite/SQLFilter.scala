package lightdb.sqlite

import lightdb.Document
import lightdb.query.Filter

case class SQLFilter[F, D <: Document[D]](fieldName: String, condition: String, value: F) extends Filter[D]