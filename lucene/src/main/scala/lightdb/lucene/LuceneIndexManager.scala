package lightdb.lucene

import cats.effect.IO
import lightdb.Document
import lightdb.index.IndexManager
import lightdb.lucene.index._

case class LuceneIndexManager[D <: Document[D]](indexSupport: LuceneSupport[D]) extends IndexManager[D] {
  def apply(name: String): IndexedFieldBuilder = IndexedFieldBuilder(name)

  override def count(): IO[Int] = indexSupport.indexer.count()

  case class IndexedFieldBuilder(fieldName: String) {
    def tokenized(f: D => String): TokenizedField[D] = TokenizedField(fieldName, indexSupport, f)
    def string(f: D => String, store: Boolean = false): StringField[D] = StringField(fieldName, indexSupport, f, store)
    def int(f: D => Int): IntField[D] = IntField(fieldName, indexSupport, f)
    def long(f: D => Long): LongField[D] = LongField(fieldName, indexSupport, f)
    def float(f: D => Float): FloatField[D] = FloatField(fieldName, indexSupport, f)
    def double(f: D => Double): DoubleField[D] = DoubleField(fieldName, indexSupport, f)
    def bigDecimal(f: D => BigDecimal): BigDecimalField[D] = BigDecimalField(fieldName, indexSupport, f)
  }
}
