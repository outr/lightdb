package lightdb.error

import lightdb.Query
import lightdb.field.Field

case class NonIndexedFieldException(query: Query[_, _], fields: List[Field[_, _]]) extends RuntimeException(s"Attempting to execute a query with non-indexed fields in an indexed store mode. Not indexed fields: ${fields.map(_.name).mkString(", ")}")
