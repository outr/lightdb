package lightdb.lucene

import lightdb.Document
import lightdb.query.Filter
import org.apache.lucene.search.{Query => LuceneQuery}

case class LuceneFilter[D <: Document[D]](asQuery: () => LuceneQuery) extends Filter[D]
