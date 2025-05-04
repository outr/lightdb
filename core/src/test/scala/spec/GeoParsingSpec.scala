package spec

import fabric.io.JsonParser
import lightdb.spatial.Geo
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

//@EmbeddedTest
class GeoParsingSpec extends AnyWordSpec with Matchers {
  "Geo Parsing" should {
    "parse a String point" in {
      val geo = Geo.parseString("POINT(-103.793467263 32.331700182)")
      geo should be(Geo.Point(
        latitude = 32.331700182,
        longitude = -103.793467263
      ))
    }
    "parse a JSON point" in {
      val json = JsonParser("""{"crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4269"}}, "type": "Point", "coordinates": [-103.793467263, 32.331700182]}""")
      val geo = Geo.parse(json)
      geo should be(Geo.Point(
        latitude = 32.331700182,
        longitude = -103.793467263
      ))
    }
    "parse a String polygon" in {
      val geo = Geo.parseString("""POLYGON((-104.260036453 32.598867934,-104.242881455 32.598815009,-104.225751274 32.598860916,-104.225734354 32.602483742,-104.242847836 32.602447649,-104.260003704 32.602492467,-104.260003848 32.602476407,-104.260036453 32.598867934))""")
      geo should be(Geo.Polygon(List(
        Geo.Point(longitude = -104.260036453, latitude = 32.598867934),
        Geo.Point(longitude = -104.242881455, latitude = 32.598815009),
        Geo.Point(longitude = -104.225751274, latitude = 32.598860916),
        Geo.Point(longitude = -104.225734354, latitude = 32.602483742),
        Geo.Point(longitude = -104.242847836, latitude = 32.602447649),
        Geo.Point(longitude = -104.260003704, latitude = 32.602492467),
        Geo.Point(longitude = -104.260003848, latitude = 32.602476407),
        Geo.Point(longitude = -104.260036453, latitude = 32.598867934)
      )))
    }
    "parse a JSON polygon" in {
      val json = JsonParser("""{"crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4269"}}, "type": "GeometryCollection", "geometries": [{"type": "Polygon", "coordinates": [[[-104.260036453, 32.598867934], [-104.242881455, 32.598815009], [-104.225751274, 32.598860916], [-104.225734354, 32.602483742], [-104.242847836, 32.602447649], [-104.260003704, 32.602492467], [-104.260003848, 32.602476407], [-104.260036453, 32.598867934]]]}]}""")
      val geo = Geo.parse(json)
      geo should be(Geo.Polygon(List(
        Geo.Point(longitude = -104.260036453, latitude = 32.598867934),
        Geo.Point(longitude = -104.242881455, latitude = 32.598815009),
        Geo.Point(longitude = -104.225751274, latitude = 32.598860916),
        Geo.Point(longitude = -104.225734354, latitude = 32.602483742),
        Geo.Point(longitude = -104.242847836, latitude = 32.602447649),
        Geo.Point(longitude = -104.260003704, latitude = 32.602492467),
        Geo.Point(longitude = -104.260003848, latitude = 32.602476407),
        Geo.Point(longitude = -104.260036453, latitude = 32.598867934)
      )))
    }
    "parse a String multipolygon" in {
      val geo = Geo.parseString("""MULTIPOLYGON(((-103.894776713 32.000149899,-103.890489841 32.000150665,-103.890490162 32.002410672,-103.894776629 32.002407742,-103.894776713 32.000149899)),((-103.894818052 32.020661933,-103.894807646 32.017011035,-103.894797235 32.013359954,-103.894786827 32.009709053,-103.894776598 32.006058151,-103.894776611 32.004581088,-103.894776574 32.003104024,-103.890490727 32.003106436,-103.890490669 32.006061933,-103.890527994 32.020667698,-103.894818052 32.020661933)))""")
      geo should be(Geo.MultiPolygon(List(
        Geo.Polygon(List(
          Geo.Point(longitude = -103.894776713, latitude = 32.000149899),
          Geo.Point(longitude = -103.890489841, latitude = 32.000150665),
          Geo.Point(longitude = -103.890490162, latitude = 32.002410672),
          Geo.Point(longitude = -103.894776629, latitude = 32.002407742),
          Geo.Point(longitude = -103.894776713, latitude = 32.000149899)
        )),
        Geo.Polygon(List(
          Geo.Point(longitude = -103.894818052, latitude = 32.020661933),
          Geo.Point(longitude = -103.894807646, latitude = 32.017011035),
          Geo.Point(longitude = -103.894797235, latitude = 32.013359954),
          Geo.Point(longitude = -103.894786827, latitude = 32.009709053),
          Geo.Point(longitude = -103.894776598, latitude = 32.006058151),
          Geo.Point(longitude = -103.894776611, latitude = 32.004581088),
          Geo.Point(longitude = -103.894776574, latitude = 32.003104024),
          Geo.Point(longitude = -103.890490727, latitude = 32.003106436),
          Geo.Point(longitude = -103.890490669, latitude = 32.006061933),
          Geo.Point(longitude = -103.890527994, latitude = 32.020667698),
          Geo.Point(longitude = -103.894818052, latitude = 32.020661933)
        ))
      )))
    }
    "parse a JSON multipolygon" in {
      val json = JsonParser("""{"crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4269"}}, "type": "GeometryCollection", "geometries": [{"type": "MultiPolygon", "coordinates": [[[[-103.894776713, 32.000149899], [-103.890489841, 32.000150665], [-103.890490162, 32.002410672], [-103.894776629, 32.002407742], [-103.894776713, 32.000149899]]], [[[-103.894818052, 32.020661933], [-103.894807646, 32.017011035], [-103.894797235, 32.013359954], [-103.894786827, 32.009709053], [-103.894776598, 32.006058151], [-103.894776611, 32.004581088], [-103.894776574, 32.003104024], [-103.890490727, 32.003106436], [-103.890490669, 32.006061933], [-103.890527994, 32.020667698], [-103.894818052, 32.020661933]]]]}]}""")
      val geo = Geo.parse(json)
      geo should be(Geo.MultiPolygon(List(
        Geo.Polygon(List(
          Geo.Point(longitude = -103.894776713, latitude = 32.000149899),
          Geo.Point(longitude = -103.890489841, latitude = 32.000150665),
          Geo.Point(longitude = -103.890490162, latitude = 32.002410672),
          Geo.Point(longitude = -103.894776629, latitude = 32.002407742),
          Geo.Point(longitude = -103.894776713, latitude = 32.000149899)
        )),
        Geo.Polygon(List(
          Geo.Point(longitude = -103.894818052, latitude = 32.020661933),
          Geo.Point(longitude = -103.894807646, latitude = 32.017011035),
          Geo.Point(longitude = -103.894797235, latitude = 32.013359954),
          Geo.Point(longitude = -103.894786827, latitude = 32.009709053),
          Geo.Point(longitude = -103.894776598, latitude = 32.006058151),
          Geo.Point(longitude = -103.894776611, latitude = 32.004581088),
          Geo.Point(longitude = -103.894776574, latitude = 32.003104024),
          Geo.Point(longitude = -103.890490727, latitude = 32.003106436),
          Geo.Point(longitude = -103.890490669, latitude = 32.006061933),
          Geo.Point(longitude = -103.890527994, latitude = 32.020667698),
          Geo.Point(longitude = -103.894818052, latitude = 32.020661933)
        ))
      )))
    }
    "parse a multi-level geometry collection" in {
      val json = JsonParser("""{"crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4269"}}, "type": "GeometryCollection", "geometries": [{"type": "GeometryCollection", "geometries": [{"type": "Polygon", "coordinates": [[[-103.520208844, 33.519270954], [-103.520208848, 33.519270999], [-103.52011766, 33.526514651], [-103.520117659, 33.526514651], [-103.519644278, 33.540956508], [-103.519644272, 33.540956552], [-103.519644229, 33.540956557], [-103.510915724, 33.541056059], [-103.510915678, 33.541056055], [-103.510915675, 33.54105601], [-103.511152616, 33.533861856], [-103.51115433, 33.533809829], [-103.511271087, 33.530264804], [-103.511389559, 33.526667664], [-103.511389559, 33.526667642], [-103.511472584, 33.519463527], [-103.511472999, 33.519427506], [-103.511473004, 33.519427455], [-103.511473049, 33.51942745], [-103.520208798, 33.51927095], [-103.520208844, 33.519270954]]]}, {"type": "LineString", "coordinates": [[-103.52011766, 33.526514651], [-103.520117662, 33.526514651]]}]}]}""")
      val geo = Geo.parse(json)
      geo should be(Geo.GeometryCollection(List(
        Geo.Polygon(List(
          Geo.Point(33.519270954, -103.520208844),
          Geo.Point(33.519270999, -103.520208848),
          Geo.Point(33.526514651, -103.52011766),
          Geo.Point(33.526514651, -103.520117659),
          Geo.Point(33.540956508, -103.519644278),
          Geo.Point(33.540956552, -103.519644272),
          Geo.Point(33.540956557, -103.519644229),
          Geo.Point(33.541056059, -103.510915724),
          Geo.Point(33.541056055, -103.510915678),
          Geo.Point(33.54105601, -103.510915675),
          Geo.Point(33.533861856, -103.511152616),
          Geo.Point(33.533809829, -103.51115433),
          Geo.Point(33.530264804, -103.511271087),
          Geo.Point(33.526667664, -103.511389559),
          Geo.Point(33.526667642, -103.511389559),
          Geo.Point(33.519463527, -103.511472584),
          Geo.Point(33.519427506, -103.511472999),
          Geo.Point(33.519427455, -103.511473004),
          Geo.Point(33.51942745, -103.511473049),
          Geo.Point(33.51927095, -103.520208798),
          Geo.Point(33.519270954, -103.520208844)
        )),
        Geo.Line(List(
          Geo.Point(33.526514651, -103.52011766),
          Geo.Point(33.526514651, -103.520117662)
        ))
      )))
    }
  }
}
