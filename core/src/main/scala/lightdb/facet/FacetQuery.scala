package lightdb.facet

import lightdb.doc.Document
import lightdb.field.Field.FacetField

case class FacetQuery[Doc <: Document[Doc]](field: FacetField[Doc],
                                            path: List[String],
                                            childrenLimit: Option[Int],
                                            dimsLimit: Option[Int])
