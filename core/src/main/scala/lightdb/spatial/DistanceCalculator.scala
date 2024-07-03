package lightdb.spatial

import lightdb.distance._

import scala.math._

object DistanceCalculator {
  val EarthRadiusMeters: Int = 6371000 // Earth's radius in meters

  // Calculate the Haversine distance between two points in meters
  def haversineDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val dLat = toRadians(lat2 - lat1)
    val dLon = toRadians(lon2 - lon1)
    val a = sin(dLat / 2) * sin(dLat / 2) + cos(toRadians(lat1)) * cos(toRadians(lat2)) * sin(dLon / 2) * sin(dLon / 2)
    val c = 2 * atan2(sqrt(a), sqrt(1 - a))
    EarthRadiusMeters * c
  }

  def apply(p1: GeoPoint, p2: GeoPoint): Distance = haversineDistance(
    lat1 = p1.latitude,
    lon1 = p1.longitude,
    lat2 = p2.latitude,
    lon2 = p2.longitude
  ).meters
}