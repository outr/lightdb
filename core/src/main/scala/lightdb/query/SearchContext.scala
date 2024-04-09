package lightdb.query

import lightdb.index.IndexSupport
import lightdb.Document

case class SearchContext[D <: Document[D]](indexSupport: IndexSupport[D])