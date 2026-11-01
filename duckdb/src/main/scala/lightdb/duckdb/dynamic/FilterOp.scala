package lightdb.duckdb.dynamic

import fabric.rw.*

/** What a filter compares a column with: equal, not equal, greater, at least, less, at most, holds the words (text, any case), starts with, or is one of. */
enum FilterOp {
  case Eq, Ne, Gt, Gte, Lt, Lte, Contains, StartsWith, In
}

object FilterOp {
  given RW[FilterOp] = RW.string(_.toString, FilterOp.valueOf, className = Some("lightdb.duckdb.dynamic.FilterOp"))
}
