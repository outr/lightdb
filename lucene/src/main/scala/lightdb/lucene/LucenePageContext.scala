package lightdb.lucene

import cats.effect.IO
import lightdb.Document
import lightdb.query.{PageContext, PagedResults, SearchContext}
import org.apache.lucene.search.ScoreDoc

case class LucenePageContext[D <: Document[D]](context: SearchContext[D],
                                               lastScoreDoc: Option[ScoreDoc]) extends PageContext[D]