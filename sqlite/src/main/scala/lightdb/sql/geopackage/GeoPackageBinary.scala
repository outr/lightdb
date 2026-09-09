package lightdb.sql.geopackage

import lightdb.spatial.*

import java.nio.{ByteBuffer, ByteOrder}

/**
 * Encodes a LightDB [[Geo]] as GeoPackage Binary: the 8-byte GeoPackage header (magic "GP", version,
 * flags, SRS id) followed by standard OGC Well-Known Binary. Coordinates are written as (x, y) =
 * (longitude, latitude), which is what WKB and GIS tools expect; LightDB's [[Point]] holds them as
 * (latitude, longitude). No envelope is written (envelope indicator 0); readers compute it.
 *
 * This is the export encoding only. LightDB's own SQLite store keeps geometry as GeoJSON text and
 * queries it through custom SQL functions; that is unaffected.
 */
object GeoPackageBinary {
  private val WkbPoint = 1
  private val WkbLineString = 2
  private val WkbPolygon = 3
  private val WkbMultiPoint = 4
  private val WkbMultiLineString = 5
  private val WkbMultiPolygon = 6
  private val WkbGeometryCollection = 7

  def encode(geo: Geo, srid: Int): Array[Byte] = {
    val wkb = encodeWkb(geo)
    val out = ByteBuffer.allocate(8 + wkb.length).order(ByteOrder.LITTLE_ENDIAN)
    out.put('G'.toByte).put('P'.toByte)
    out.put(0.toByte)             // version 1
    out.put(0x01.toByte)          // flags: little-endian, no envelope, not empty, binary type = standard
    out.putInt(srid)
    out.put(wkb)
    out.array()
  }

  /** Every vertex of a geometry, for envelope computation. */
  def pointsOf(geo: Geo): List[Point] = geo match {
    case p: Point => List(p)
    case l: Line => l.points
    case p: Polygon => p.points
    case mp: MultiPoint => mp.points
    case ml: MultiLine => ml.lines.flatMap(_.points)
    case mp: MultiPolygon => mp.polygons.flatMap(_.points)
    case gc: GeometryCollection => gc.geometries.flatMap(pointsOf)
    case other => List(other.center)
  }

  private def encodeWkb(geo: Geo): Array[Byte] = {
    val buf = new java.io.ByteArrayOutputStream()
    writeGeo(buf, geo)
    buf.toByteArray
  }

  private def writeGeo(out: java.io.ByteArrayOutputStream, geo: Geo): Unit = {
    def header(kind: Int): Unit = {
      out.write(1) // little-endian
      out.write(le(4).putInt(kind).array())
    }
    def point(p: Point): Unit = out.write(le(16).putDouble(p.longitude).putDouble(p.latitude).array())
    def ring(points: List[Point]): Unit = {
      // A WKB ring must be closed (first == last).
      val closed = if (points.nonEmpty && points.head != points.last) points :+ points.head else points
      out.write(le(4).putInt(closed.size).array())
      closed.foreach(point)
    }
    geo match {
      case p: Point => header(WkbPoint); point(p)
      case l: Line => header(WkbLineString); out.write(le(4).putInt(l.points.size).array()); l.points.foreach(point)
      case p: Polygon => header(WkbPolygon); out.write(le(4).putInt(1).array()); ring(p.points)
      case mp: MultiPoint => header(WkbMultiPoint); out.write(le(4).putInt(mp.points.size).array()); mp.points.foreach(p => writeGeo(out, p))
      case ml: MultiLine => header(WkbMultiLineString); out.write(le(4).putInt(ml.lines.size).array()); ml.lines.foreach(l => writeGeo(out, l))
      case mp: MultiPolygon => header(WkbMultiPolygon); out.write(le(4).putInt(mp.polygons.size).array()); mp.polygons.foreach(p => writeGeo(out, p))
      case gc: GeometryCollection => header(WkbGeometryCollection); out.write(le(4).putInt(gc.geometries.size).array()); gc.geometries.foreach(g => writeGeo(out, g))
      case other => header(WkbPoint); point(other.center)
    }
  }

  private def le(n: Int): ByteBuffer = ByteBuffer.allocate(n).order(ByteOrder.LITTLE_ENDIAN)
}
