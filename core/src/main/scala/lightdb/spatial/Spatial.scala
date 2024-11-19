package lightdb.spatial

import lightdb.distance._
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, LineString, Polygon}
import org.locationtech.spatial4j.context.SpatialContext
import org.locationtech.spatial4j.context.jts.JtsSpatialContext
import org.locationtech.spatial4j.distance.DistanceUtils
import org.locationtech.spatial4j.shape
import org.locationtech.spatial4j.shape.Shape
import org.locationtech.spatial4j.shape.ShapeFactory.{LineStringBuilder, PolygonBuilder}
import org.locationtech.spatial4j.shape.jts.JtsGeometry

object Spatial {
  private lazy val context = JtsSpatialContext.GEO
  private lazy val factory = new GeometryFactory()

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

  private def toShape(g: Geo): Geometry = g match {
    case Geo.Point(lat, lon) => factory.createPoint(new Coordinate(lon, lat))
    case Geo.MultiPoint(points) =>
      factory.createMultiPoint(points.map {
        case Geo.Point(lat, lon) => factory.createPoint(new Coordinate(lon, lat))
        case _ => throw new IllegalArgumentException("Invalid point in MultiPoint")
      }.toArray)
    case line: Geo.Line => factory.createLineString(line.points.map {
      case Geo.Point(lat, lon) => new Coordinate(lon, lat)
    }.toArray)
    case Geo.MultiLine(lines) => factory.createMultiLineString(lines.map(toShape).map {
      case l: LineString => l
    }.toArray)
    case polygon: Geo.Polygon => factory.createPolygon(polygon.points.map {
      case Geo.Point(lat, lon) => new Coordinate(lon, lat)
    }.toArray)
    case Geo.MultiPolygon(polygons) => factory.createMultiPolygon(polygons.map(toShape).map {
      case p: Polygon => p
    }.toArray)
    case Geo.GeometryCollection(list) => factory.createGeometryCollection(
      list.map(toShape).toArray
    )
  }

  def relation(g1: Geo, g2: Geo): SpatialRelation = {
    val s1 = new JtsGeometry(toShape(g1), context, false, false)
    val s2 = new JtsGeometry(toShape(g2), context, false, false)
    s1.relate(s2) match {
      case shape.SpatialRelation.WITHIN => SpatialRelation.Within
      case shape.SpatialRelation.CONTAINS => SpatialRelation.Contains
      case shape.SpatialRelation.INTERSECTS => SpatialRelation.Intersects
      case shape.SpatialRelation.DISJOINT => SpatialRelation.Disjoint
    }
  }

  def overlap(g1: Geo, g2: Geo): Boolean = relation(g1, g2) != SpatialRelation.Disjoint
}
