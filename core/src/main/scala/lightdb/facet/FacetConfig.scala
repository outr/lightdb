package lightdb.facet

case class FacetConfig(hierarchical: Boolean = false,
                       multiValued: Boolean = false,
                       requireDimCount: Boolean = false)
