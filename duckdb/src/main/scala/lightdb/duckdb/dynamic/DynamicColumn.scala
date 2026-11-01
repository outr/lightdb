package lightdb.duckdb.dynamic

import fabric.rw.*

/** A column of a dynamic table: its `name` (as people and agents say it) and `kind`. */
final case class DynamicColumn(name: String, kind: ColumnKind = ColumnKind.Text, description: String = "") derives RW
