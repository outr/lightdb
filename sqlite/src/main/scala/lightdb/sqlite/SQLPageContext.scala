package lightdb.sqlite

import lightdb.Document
import lightdb.query.{PageContext, SearchContext}

case class SQLPageContext[D <: Document[D]](context: SearchContext[D]) extends PageContext[D]