package lightdb.duckdb.dynamic

import fabric.*
import fabric.rw.*

/** `column` `op` `value` (`In` takes an array). */
final case class ColumnFilter(column: String, op: FilterOp, value: Json) derives RW
