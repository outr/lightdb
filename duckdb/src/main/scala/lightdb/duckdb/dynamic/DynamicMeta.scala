package lightdb.duckdb.dynamic

import fabric.rw.*

/** What a table is, kept beside its database so it opens again as it was. */
private[dynamic] final case class DynamicMeta(name: String, columns: List[DynamicColumn], readOnly: Boolean = false) derives RW
