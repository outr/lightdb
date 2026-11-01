package lightdb.duckdb.dynamic

import fabric.rw.*

/** What a dynamic table's column holds. Text is any text; Integer a whole number; Number a decimal; Boolean true or false. */
enum ColumnKind {
  case Text, Integer, Number, Boolean
}

object ColumnKind {
  given RW[ColumnKind] = RW.string(_.toString, ColumnKind.valueOf, className = Some("lightdb.duckdb.dynamic.ColumnKind"))
}
