package benchmark.jmh.complete

import fabric.rw.*
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id

/** Synthetic doc shape exercised by every benchmark. Field choices:
 *
 *  - `name`: indexed string — supports term/equality and sort (where the backend allows).
 *  - `age`: indexed int — supports range filters.
 *  - `city`: indexed optional string — exercises null-vs-set semantics.
 *  - `bio`: tokenized text — Lucene/Tantivy only; SQL backends fall back to a regular string.
 *
 *  Kept small (~200B per doc) so disk writes don't dominate every benchmark — write throughput
 *  on JSON-serialized backends is heavily field-count sensitive.
 */
case class BenchDoc(
  name: String,
  age: Int,
  city: Option[String],
  bio: String,
  _id: Id[BenchDoc] = BenchDoc.id()
) extends Document[BenchDoc]

object BenchDoc extends DocumentModel[BenchDoc] with JsonConversion[BenchDoc] {
  override implicit val rw: RW[BenchDoc] = RW.gen

  val name : I[String]         = field.index("name", _.name)
  val age  : I[Int]             = field.index("age", _.age)
  val city : I[Option[String]]  = field.index("city", _.city)
  // Tokenized for Lucene/Tantivy; non-FTS backends just see it as a stored String.
  val bio  : T                  = field.tokenized("bio", _.bio)
}
