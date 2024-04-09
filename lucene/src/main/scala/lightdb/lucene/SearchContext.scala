package lightdb.lucene

import lightdb.{Collection, Document}
import org.apache.lucene.search.IndexSearcher

case class SearchContext[D <: Document[D]](collection: Collection[D],
                                           private[lightdb] val indexSearcher: IndexSearcher)