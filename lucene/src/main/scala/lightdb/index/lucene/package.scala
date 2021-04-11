package lightdb.index

import com.outr.lucene4s.ImplicitValueSupport
import com.outr.lucene4s.field.FieldType
import com.outr.lucene4s.field.value.support.{StringBackedValueSupport, ValueSupport}
import lightdb.{Document, Id}
import lightdb.field.Field

package object lucene extends ImplicitValueSupport {
  private object _idSupport extends StringBackedValueSupport[Id[Any]] {
    override def toString(value: Id[Any]): String = value.value

    override def fromString(s: String): Id[Any] = Id[Any](s)
  }

  implicit def idSupport[D]: ValueSupport[Id[D]] = _idSupport.asInstanceOf[ValueSupport[Id[D]]]

  implicit class FieldExtras[D <: Document[D], F](field: Field[D, F]) {
    def indexed(fieldType: FieldType = FieldType.Stored,
                fullTextSearchable: Boolean = true,     // TODO: use default from collection?
                sortable: Boolean = true)(implicit vs: ValueSupport[F]): Field[D, F] = {
      field.withFeature(IndexFeature[F](fieldType, fullTextSearchable, sortable, vs))
    }
  }
}