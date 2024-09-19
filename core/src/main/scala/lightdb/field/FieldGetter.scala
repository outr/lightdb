package lightdb.field

import lightdb.doc.Document

import scala.language.implicitConversions

trait FieldGetter[Doc <: Document[Doc], V] {
  def apply(doc: Doc, field: Field[Doc, V]): V
}

object FieldGetter {
  implicit def function2Getter[Doc <: Document[Doc], V](f: Doc => V): FieldGetter[Doc, V] = new FieldGetter[Doc, V] {
    override def apply(doc: Doc, field: Field[Doc, V]): V = f(doc)
  }
}