package lightdb.spatial

import lightdb.distance._
import org.locationtech.spatial4j.context.SpatialContext
import org.locationtech.spatial4j.distance.DistanceUtils

object Spatial {
  private lazy val context = SpatialContext.GEO

  def distance(p1: Geo.Point, p2: Geo.Point): Distance = {
    val point1 = context.getShapeFactory.pointLatLon(p1.latitude, p2.longitude)
    val point2 = context.getShapeFactory.pointLatLon(p2.latitude, p2.longitude)
    val degrees = context.calcDistance(point1, point2)
    val distance = DistanceUtils.degrees2Dist(degrees, DistanceUtils.EARTH_MEAN_RADIUS_KM)
    distance.kilometers
  }
}
