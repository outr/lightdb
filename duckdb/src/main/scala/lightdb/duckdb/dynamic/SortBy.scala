package lightdb.duckdb.dynamic

import fabric.rw.*

/** Order by `column`, by value for a number column and as text for a text one. */
final case class SortBy(column: String, descending: Boolean = false) derives RW
