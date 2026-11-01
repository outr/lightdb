package lightdb.duckdb.dynamic

import fabric.rw.*

/**
 * Which rows, in what order: every filter must hold; `search` words must each be in some text column (any case); `offset`/`limit`
 * page the result (`limit` is held to 1 to 5,000).
 */
final case class DynamicQuery(filters: List[ColumnFilter] = Nil, search: String = "", sort: List[SortBy] = Nil, offset: Int = 0, limit: Int = 100) derives RW
