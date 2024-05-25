package lightdb.lucene

import lightdb.Document
import lightdb.query.Filter
import org.apache.lucene.search.{BooleanClause, BooleanQuery, Query => LuceneQuery}

case class LuceneFilter[D <: Document[D]](asQuery: () => LuceneQuery) extends Filter[D] {
  override def &&(that: Filter[D]): Filter[D] = LuceneFilter[D](() => {
    val b = new BooleanQuery.Builder
    b.add(this.asQuery(), BooleanClause.Occur.MUST)
    b.add(that.asInstanceOf[LuceneFilter[D]].asQuery(), BooleanClause.Occur.MUST)
    b.build()
  })
}