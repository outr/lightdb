package lightdb.facet

import lightdb.field.Field.FacetField
import lightdb.doc.Document

case class FacetQuery[Doc <: Document[Doc]](field: FacetField[Doc],
                                            path: List[String],
                                            childrenLimit: Option[Int],
                                            dimsLimit: Option[Int])
