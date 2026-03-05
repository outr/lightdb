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

  /**
   * Returns 1 if any geometry in geosJson spatially contains the geometry in otherJson, 0 otherwise.
   */
  def spatialContains(geosJson: String, otherJson: String): Int = {
    if geosJson == null || otherJson == null then 0
    else {
      val geos = parseGeos(geosJson)
      val other = parseGeos(otherJson)
      val contains = geos.exists(g => other.exists(o => Spatial.relation(g, o) == lightdb.spatial.SpatialRelation.Contains))
      if contains then 1 else 0
    }
  }

  /**
   * Returns 1 if any geometry in geosJson spatially intersects (is not disjoint with) the geometry in otherJson, 0 otherwise.
   */
  def spatialIntersects(geosJson: String, otherJson: String): Int = {
    if geosJson == null || otherJson == null then 0
    else {
      val geos = parseGeos(geosJson)
      val other = parseGeos(otherJson)
      val intersects = geos.exists(g => other.exists(o => Spatial.overlap(g, o)))
      if intersects then 1 else 0
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

