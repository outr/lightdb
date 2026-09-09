package spec

import fabric.*
import lightdb.spatial.*
import lightdb.sql.geopackage.{GeoPackage, GeoPackageBinary}
import lightdb.sql.geopackage.GeoPackage.{ColumnType, Feature, GeometryType, Layer}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.{Files, Path}
import java.sql.DriverManager
import scala.util.Using

/**
 * Writes a GeoPackage and reads it back through plain SQLite, checking what a GIS reader (GeoServer,
 * QGIS, ogr2ogr) checks: the OGC metadata tables, the SRS, and that each geometry blob is valid
 * GeoPackage Binary whose WKB decodes to the coordinates that went in.
 */
class GeoPackageSpec extends AnyWordSpec with Matchers {
  private val unit = Polygon.lonLat(-91.5, 30.1, -91.4, 30.1, -91.4, 30.2, -91.5, 30.2)
  private val well = Point(latitude = 30.15, longitude = -91.45)

  private val path: Path = Files.createTempDirectory("gpkg").resolve("test.gpkg")

  private lazy val counts = GeoPackage.write(path, List(
    Layer("units", GeometryType.Polygon, List("name" -> ColumnType.Text, "acres" -> ColumnType.Real),
      Iterator(Feature(Some(unit), List(str("CV RA SUA"), num(640.0))))),
    Layer("wells", GeometryType.Point, List("api" -> ColumnType.Text, "serial" -> ColumnType.Integer),
      Iterator(Feature(Some(well), List(str("17-007-88076"), num(975702))), Feature(None, List(str("no-geom"), Null))))
  ))

  private def sql[T](q: String)(f: java.sql.ResultSet => T): List[T] =
    Using.resource(DriverManager.getConnection(s"jdbc:sqlite:$path")) { c =>
      Using.resource(c.createStatement().executeQuery(q)) { rs =>
        val b = List.newBuilder[T]; while (rs.next()) b += f(rs); b.result()
      }
    }

  "GeoPackage" should {
    "write the layers and report counts" in {
      counts should be(Map("units" -> 1L, "wells" -> 2L))
    }
    "carry the GeoPackage application id and required metadata tables" in {
      sql("PRAGMA application_id")(_.getInt(1)) should be(List(1196444487))
      sql("SELECT table_name, data_type, srs_id FROM gpkg_contents ORDER BY table_name")(rs => (rs.getString(1), rs.getString(2), rs.getInt(3))) should be(
        List(("units", "features", 4326), ("wells", "features", 4326)))
      sql("SELECT table_name, column_name, geometry_type_name FROM gpkg_geometry_columns ORDER BY table_name")(rs => (rs.getString(1), rs.getString(2), rs.getString(3))) should be(
        List(("units", "geom", "POLYGON"), ("wells", "geom", "POINT")))
      sql("SELECT organization, organization_coordsys_id FROM gpkg_spatial_ref_sys WHERE srs_id = 4326")(rs => (rs.getString(1), rs.getInt(2))) should be(List(("EPSG", 4326)))
    }
    "record the layer extent in lon/lat" in {
      sql("SELECT min_x, min_y, max_x, max_y FROM gpkg_contents WHERE table_name = 'units'")(rs => (rs.getDouble(1), rs.getDouble(2), rs.getDouble(3), rs.getDouble(4))) should be(
        List((-91.5, 30.1, -91.4, 30.2)))
    }
    "store attributes with their types" in {
      sql("SELECT name, acres FROM units")(rs => (rs.getString(1), rs.getDouble(2))) should be(List(("CV RA SUA", 640.0)))
      sql("SELECT api, serial FROM wells ORDER BY fid")(rs => (rs.getString(1), rs.getObject(2))) should be(
        List(("17-007-88076", 975702L), ("no-geom", null)))
    }
    "encode a point as GeoPackage Binary over WKB with (lon, lat) order" in {
      val blob = sql("SELECT geom FROM wells WHERE api = '17-007-88076'")(_.getBytes(1)).head
      blob(0).toChar should be('G'); blob(1).toChar should be('P')
      ByteBuffer.wrap(blob, 4, 4).order(ByteOrder.LITTLE_ENDIAN).getInt should be(4326)
      val wkb = ByteBuffer.wrap(blob, 8, blob.length - 8).order(ByteOrder.LITTLE_ENDIAN)
      wkb.get() should be(1.toByte)           // little-endian
      wkb.getInt should be(1)                  // wkbPoint
      wkb.getDouble should be(-91.45)          // x = longitude
      wkb.getDouble should be(30.15)           // y = latitude
    }
    "encode a polygon as a closed ring" in {
      val blob = sql("SELECT geom FROM units")(_.getBytes(1)).head
      val wkb = ByteBuffer.wrap(blob, 8, blob.length - 8).order(ByteOrder.LITTLE_ENDIAN)
      wkb.get(); wkb.getInt should be(3)       // wkbPolygon
      wkb.getInt should be(1)                  // one ring
      val n = wkb.getInt; n should be(5)       // 4 vertices + closing repeat
      val first = (wkb.getDouble, wkb.getDouble)
      (1 until n - 1).foreach(_ => { wkb.getDouble; wkb.getDouble })
      (wkb.getDouble, wkb.getDouble) should be(first)
    }
    "leave a feature with no geometry as NULL" in {
      sql("SELECT geom IS NULL FROM wells WHERE api = 'no-geom'")(_.getInt(1)) should be(List(1))
    }
  }
}
