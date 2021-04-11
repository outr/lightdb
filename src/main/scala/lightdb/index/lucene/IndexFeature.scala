package lightdb.index.lucene

import com.outr.lucene4s.Lucene
import com.outr.lucene4s.field.{Field => LuceneField}
import com.outr.lucene4s.field.FieldType
import com.outr.lucene4s.field.value.support.ValueSupport
import lightdb.field.FieldFeature

case class IndexFeature[F](fieldType: FieldType,
                           fullTextSearchable: Boolean,
                           sortable: Boolean,
                           valueSupport: ValueSupport[F]) extends FieldFeature {
  def createField(name: String, lucene: Lucene): LuceneField[F] = lucene.create.field[F](
    name = name,
    fieldType = fieldType,
    fullTextSearchable = fullTextSearchable,
    sortable = sortable
  )(valueSupport)
}