package lightdb.duckdb.dynamic

import fabric.*
import fabric.rw.*

/** A row: its id and its values by column name (absent: no value). */
final case class DynamicRow(id: String, values: Map[String, Json]) derives RW
