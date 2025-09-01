package lightdb

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field

case class CompositeIndex[Doc <: Document[Doc]](name: String,
                                                model: DocumentModel[Doc],
                                                fields: List[Field[Doc, _]])
