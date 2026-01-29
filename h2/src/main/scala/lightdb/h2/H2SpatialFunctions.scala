package lightdb.h2

import fabric.io.JsonParser
import fabric.rw.{Asable, listRW, Convertible, doubleRW}
import lightdb.spatial.{Geo, Spatial}
import fabric.io.JsonFormatter

object H2SpatialFunctions {
  /**
   * Returns a JSON array of distances (in meters) between each geometry in geosJson and each geometry in otherJson.
   * Mirrors the SQLite behavior used by Conversion.Distance.
   */
  def distanceJson(geosJson: String, otherJson: String): String = {
    if geosJson == null || otherJson == null then null
    else {
      val geos = parseGeos(geosJson)
      val others = parseGeos(otherJson)
      val distances: List[Double] = for
        g <- geos
        o <- others
      yield Spatial.distance(g, o).valueInMeters
      JsonFormatter.Compact(distances.json)
    }
  }

  /**
   * Returns the minimum distance (in meters) between the supplied geometries.
   */
  def distanceMin(geosJson: String, otherJson: String): java.lang.Double = {
    if geosJson == null || otherJson == null then null
    else {
      val geos = parseGeos(geosJson)
      val others = parseGeos(otherJson)
      val mins = for
        g <- geos
        o <- others
      yield Spatial.distance(g, o).valueInMeters
      mins.minOption.map(double2Double).orNull
    }
  }

  private def parseGeos(jsonStr: String): List[Geo] = {
    if jsonStr == null then Nil
    else {
      val json = JsonParser(jsonStr)
      json match {
        case arr: fabric.Arr => arr.as[List[Geo]]
        case _ => json.as[Geo] :: Nil
      }
    }
  }
}

