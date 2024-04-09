package lightdb.query

import lightdb.Document
import org.apache.lucene.search.{Query => LuceneQuery}

trait Filter[D <: Document[D]] {
  protected[lightdb] def asQuery: LuceneQuery
}

object Filter {
  def apply[D <: Document[D]](f: => LuceneQuery): Filter[D] = new Filter[D] {
    override protected[lightdb] def asQuery: LuceneQuery = f
  }
}