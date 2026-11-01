package lightdb.duckdb.dynamic

import fabric.rw.*

/** A page of rows, and how many rows match in all. */
final case class DynamicPage(rows: Vector[DynamicRow], total: Int) derives RW
