package lightdb.index

import lightdb.field.FieldFeature
import org.apache.lucene.document.{Document => LuceneDoc}

trait LuceneIndexFeature[T] extends FieldFeature {
  def index(name: String, value: T, document: LuceneDoc): Unit
}