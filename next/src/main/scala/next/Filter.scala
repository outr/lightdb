package next

sealed trait Filter[Doc] {
  def &&(that: Filter[Doc]): Filter[Doc] = (this, that) match {
    case (Filter.Combined(f1), Filter.Combined(f2)) => Filter.Combined(f1 ::: f2)
    case (_, Filter.Combined(f)) => Filter.Combined(this :: f)
    case (Filter.Combined(f), _) => Filter.Combined(f ::: List(that))
    case _ => Filter.Combined(List(this, that))
  }
}

object Filter {
  def and[Doc](filters: Filter[Doc]*): Filter[Doc] = filters.tail
    .foldLeft(filters.head)((combined, filter) => combined && filter)

  case class Equals[Doc, F](field: Field[Doc, F], value: F) extends Filter[Doc]

  case class In[Doc, F](field: Field[Doc, F], values: Seq[F]) extends Filter[Doc]

  case class Combined[Doc](filters: List[Filter[Doc]]) extends Filter[Doc]

  case class RangeLong[Doc](field: Field[Doc, Long], from: Option[Long], to: Option[Long]) extends Filter[Doc]

  case class RangeDouble[Doc](field: Field[Doc, Double], from: Option[Double], to: Option[Double]) extends Filter[Doc]

  case class Parsed[Doc, F](field: Field[Doc, F], query: String, allowLeadingWildcard: Boolean) extends Filter[Doc]

  case class Distance[Doc](field: Field[Doc, GeoPoint], from: GeoPoint, radius: Long) extends Filter[Doc]
}