package lightdb.spatial

sealed trait SpatialRelation

object SpatialRelation {
  case object Within extends SpatialRelation
  case object Contains extends SpatialRelation
  case object Intersects extends SpatialRelation
  case object Disjoint extends SpatialRelation
}