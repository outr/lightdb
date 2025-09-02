package lightdb

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field

case class CompositeIndex[Doc <: Document[Doc]](name: String,
                                                model: DocumentModel[Doc],
                                                fields: List[Field[Doc, _]],
                                                include: List[Field[Doc, _]])
