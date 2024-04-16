package lightdb.sqlite

import lightdb.index.IndexedField
import lightdb.query.Filter
import lightdb.{Collection, Document}

case class SQLIndexedField[F, D <: Document[D]](fieldName: String,
                                                collection: Collection[D],
                                                get: D => Option[F]) extends IndexedField[F, D] {
  def ===(value: F): Filter[D] = is(value)

  def is(value: F): Filter[D] = SQLFilter[F, D](fieldName, "=", value)
}
