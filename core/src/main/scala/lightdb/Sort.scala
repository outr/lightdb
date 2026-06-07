package lightdb

import lightdb.doc.Document
import lightdb.field.Field
import lightdb.spatial.{Geo, Point}
import lightdb.vector.VectorMetric

trait Sort

object Sort {
  case class BestMatch(direction: SortDirection = SortDirection.Descending) extends Sort {
    def direction(direction: SortDirection): BestMatch = copy(direction = direction)
    def ascending: BestMatch = direction(SortDirection.Ascending)
    def asc: BestMatch = direction(SortDirection.Ascending)
    def descending: BestMatch = direction(SortDirection.Descending)
    def desc: BestMatch = direction(SortDirection.Descending)
  }

  case object IndexOrder extends Sort

  case class ByField[Doc <: Document[Doc], F](field: Field[Doc, F], direction: SortDirection = SortDirection.Ascending) extends Sort {
    def direction(direction: SortDirection): ByField[Doc, F] = copy(direction = direction)
    def ascending: ByField[Doc, F] = direction(SortDirection.Ascending)
    def asc: ByField[Doc, F] = direction(SortDirection.Ascending)
    def descending: ByField[Doc, F] = direction(SortDirection.Descending)
    def desc: ByField[Doc, F] = direction(SortDirection.Descending)
  }

  case class ByDistance[Doc <: Document[Doc], G <: Geo](field: Field[Doc, List[G]],
                             from: Point,
                             direction: SortDirection = SortDirection.Ascending) extends Sort {
    def direction(direction: SortDirection): ByDistance[Doc, G] = copy(direction = direction)
    def ascending: ByDistance[Doc, G] = direction(SortDirection.Ascending)
    def asc: ByDistance[Doc, G] = direction(SortDirection.Ascending)
    def descending: ByDistance[Doc, G] = direction(SortDirection.Descending)
    def desc: ByDistance[Doc, G] = direction(SortDirection.Descending)
  }

  /**
   * Nearest-neighbor (KNN) ordering by vector similarity. Combine with `.limit(k)` to retrieve the
   * top-k most similar documents. Ascending (the default) returns the nearest first, since every
   * [[VectorMetric]] is defined so that a smaller distance means more similar.
   *
   * Native on backends with vector support (e.g. PostgreSQL + pgvector); evaluated in-memory as a
   * brute-force scan on backends without it.
   */
  case class ByVectorDistance[Doc <: Document[Doc]](field: Field[Doc, List[Double]],
                                                    vector: List[Double],
                                                    metric: VectorMetric,
                                                    direction: SortDirection = SortDirection.Ascending) extends Sort {
    def direction(direction: SortDirection): ByVectorDistance[Doc] = copy(direction = direction)
    def ascending: ByVectorDistance[Doc] = direction(SortDirection.Ascending)
    def asc: ByVectorDistance[Doc] = direction(SortDirection.Ascending)
    def descending: ByVectorDistance[Doc] = direction(SortDirection.Descending)
    def desc: ByVectorDistance[Doc] = direction(SortDirection.Descending)
  }
}