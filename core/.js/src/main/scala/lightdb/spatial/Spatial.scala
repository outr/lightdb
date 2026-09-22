package lightdb.spatial

import lightdb.distance.*

/**
 * Scala.js counterpart of the JVM `Spatial` (which is backed by spatial4j + JTS, neither available on Scala.js).
 *
 * `distance` matches the JVM result: spatial4j's geo context measures the great-circle distance between the two
 * shapes' centers with the haversine formula over the mean Earth radius, which is reproduced here exactly.
 * Shape relations (contains / intersects / within) need JTS polygon algebra and are not supported in the browser;
 * they fail loudly rather than returning an approximation that would silently change query results.
 */
object Spatial {
  private val EarthMeanRadiusKm = 6371.0087714

  def distance(p1: Geo, p2: Geo): Distance = {
    val a = p1.center
    val b = p2.center
    val lat1 = math.toRadians(a.latitude)
    val lat2 = math.toRadians(b.latitude)
    val dLat = lat2 - lat1
    val dLon = math.toRadians(b.longitude - a.longitude)
    val h = math.pow(math.sin(dLat / 2), 2) + math.cos(lat1) * math.cos(lat2) * math.pow(math.sin(dLon / 2), 2)
    val degrees = math.toDegrees(2 * math.atan2(math.sqrt(h), math.sqrt(1 - h)))
    (math.toRadians(degrees) * EarthMeanRadiusKm).kilometers
  }

  def relation(g1: Geo, g2: Geo): SpatialRelation =
    throw new UnsupportedOperationException(
      "Spatial relations (contains/intersects/within) require JTS and are not available on Scala.js"
    )

  def overlap(g1: Geo, g2: Geo): Boolean = relation(g1, g2) != SpatialRelation.Disjoint
}
