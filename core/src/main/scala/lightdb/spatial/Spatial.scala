package lightdb.spatial

import lightdb.distance._
import org.locationtech.spatial4j.context.SpatialContext
import org.locationtech.spatial4j.distance.DistanceUtils
import org.locationtech.spatial4j.shape
import org.locationtech.spatial4j.shape.Shape
import org.locationtech.spatial4j.shape.ShapeFactory.{LineStringBuilder, PolygonBuilder}

object Spatial {
  private lazy val context = SpatialContext.GEO

  def distance(p1: Geo, p2: Geo): Distance = {
    val point1 = context.getShapeFactory.pointLatLon(p1.center.latitude, p1.center.longitude)
    val point2 = context.getShapeFactory.pointLatLon(p2.center.latitude, p2.center.longitude)
    val degrees = context.calcDistance(point1, point2)
    val distance = DistanceUtils.degrees2Dist(degrees, DistanceUtils.EARTH_MEAN_RADIUS_KM)
    distance.kilometers
  }

  private def line2Builder(line: Geo.Line): LineStringBuilder =
    line.points.foldLeft(context.getShapeFactory.lineString())((b, p) =>
      b.pointLatLon(p.latitude, p.longitude)
    )

  private def polygon2Builder(polygon: Geo.Polygon): PolygonBuilder =
    polygon.points.foldLeft(context.getShapeFactory.polygon())((b, p) =>
      b.pointLatLon(p.latitude, p.longitude)
    )

  private def toShape(g: Geo): Shape = g match {
    case Geo.Point(lat, lon) => context.getShapeFactory.pointLatLon(lat, lon)
    case Geo.MultiPoint(points) => points.foldLeft(context.getShapeFactory.multiPoint())((b, p) =>
      b.pointLatLon(p.latitude, p.longitude)
    ).build()
    case line: Geo.Line => line2Builder(line).build()
    case Geo.MultiLine(lines) => lines.foldLeft(context.getShapeFactory.multiLineString())((b, l) =>
      b.add(line2Builder(l))
    ).build()
    case polygon: Geo.Polygon => polygon2Builder(polygon).build()
    case Geo.MultiPolygon(polygons) => polygons.foldLeft(context.getShapeFactory.multiPolygon())((b, p) =>
      b.add(polygon2Builder(p))
    ).build()
  }

  def relation(g1: Geo, g2: Geo): SpatialRelation = {
    val s1 = toShape(g1)
    val s2 = toShape(g2)
    s1.relate(s2) match {
      case shape.SpatialRelation.WITHIN => SpatialRelation.Within
      case shape.SpatialRelation.CONTAINS => SpatialRelation.Contains
      case shape.SpatialRelation.INTERSECTS => SpatialRelation.Intersects
      case shape.SpatialRelation.DISJOINT => SpatialRelation.Disjoint
    }
  }

  def overlap(g1: Geo, g2: Geo): Boolean = relation(g1, g2) != SpatialRelation.Disjoint
}
