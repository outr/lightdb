package lightdb.field

import lightdb.doc.Document

import scala.language.implicitConversions

trait FieldGetter[Doc <: Document[Doc], V] {
  def apply(doc: Doc, field: Field[Doc, V], state: IndexingState): V
}

object FieldGetter {
  def func[Doc <: Document[Doc], V](f: Doc => V): FieldGetter[Doc, V] = apply {
    case (doc, _, _) => f(doc)
  }

  def apply[Doc <: Document[Doc], V](f: (Doc, Field[Doc, V], IndexingState) => V): FieldGetter[Doc, V] = new FieldGetter[Doc, V] {
    override def apply(doc: Doc, field: Field[Doc, V], state: IndexingState): V = f(doc, field, state)
  }
}