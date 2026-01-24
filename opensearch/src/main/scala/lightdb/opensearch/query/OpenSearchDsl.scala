package lightdb.opensearch.query

import fabric._

object OpenSearchDsl {
  def matchAll(): Json = obj("match_all" -> obj())

  def ids(values: List[String]): Json =
    obj("ids" -> obj("values" -> arr(values.map(str): _*)))

  def prefix(field: String, value: String): Json =
    obj("prefix" -> obj(field -> obj("value" -> str(value))))

  def hasChild(childType: String, query: Json, scoreMode: String = "none"): Json =
    obj("has_child" -> obj(
      "type" -> str(childType),
      "query" -> query,
      "score_mode" -> str(scoreMode)
    ))

  def exists(field: String): Json =
    obj("exists" -> obj("field" -> str(field)))

  def term(field: String, value: Json, boost: Option[Double] = None): Json = {
    val inner = boost match {
      case Some(b) => obj("value" -> value, "boost" -> num(b))
      case None => obj("value" -> value)
    }
    obj("term" -> obj(field -> inner))
  }

  def terms(field: String, values: List[Json]): Json =
    obj("terms" -> obj(field -> arr(values: _*)))

  def range(field: String, gte: Option[Json], lte: Option[Json], gt: Option[Json] = None, lt: Option[Json] = None): Json = {
    val parts = Vector(
      gte.map(v => "gte" -> v),
      lte.map(v => "lte" -> v),
      gt.map(v => "gt" -> v),
      lt.map(v => "lt" -> v)
    ).flatten
    obj("range" -> obj(field -> obj(parts: _*)))
  }

  def regexp(field: String, value: String): Json =
    obj("regexp" -> obj(field -> obj("value" -> str(value))))

  def geoDistance(field: String, lat: Double, lon: Double, distance: String): Json =
    obj("geo_distance" -> obj(
      "distance" -> str(distance),
      field -> obj("lat" -> num(lat), "lon" -> num(lon))
    ))

  def geoBoundingBox(field: String,
                     topLeftLat: Double,
                     topLeftLon: Double,
                     bottomRightLat: Double,
                     bottomRightLon: Double): Json =
    obj("geo_bounding_box" -> obj(
      field -> obj(
        "top_left" -> obj("lat" -> num(topLeftLat), "lon" -> num(topLeftLon)),
        "bottom_right" -> obj("lat" -> num(bottomRightLat), "lon" -> num(bottomRightLon))
      )
    ))

  def boolQuery(must: List[Json] = Nil,
                filter: List[Json] = Nil,
                should: List[Json] = Nil,
                mustNot: List[Json] = Nil,
                minimumShouldMatch: Option[Int] = None): Json = {
    val parts = Vector(
      if must.nonEmpty then Some("must" -> arr(must: _*)) else None,
      if filter.nonEmpty then Some("filter" -> arr(filter: _*)) else None,
      if should.nonEmpty then Some("should" -> arr(should: _*)) else None,
      if mustNot.nonEmpty then Some("must_not" -> arr(mustNot: _*)) else None,
      minimumShouldMatch.map(msm => "minimum_should_match" -> num(msm))
    ).flatten
    obj("bool" -> obj(parts: _*))
  }

  def searchBody(filter: Json,
                 sorts: List[Json],
                 from: Int,
                 size: Option[Int],
                 trackTotalHits: Boolean,
                 trackScores: Boolean,
                 minScore: Option[Double]): Json = {
    val base = Vector(
      Some("query" -> filter),
      Some("from" -> num(from)),
      size.map(s => "size" -> num(s)),
      Some("track_total_hits" -> fabric.bool(trackTotalHits)),
      if trackScores then Some("track_scores" -> fabric.bool(true)) else None,
      minScore.map(ms => "min_score" -> num(ms))
    ).flatten

    val withSort = if sorts.nonEmpty then base :+ ("sort" -> arr(sorts: _*)) else base
    obj(withSort: _*)
  }
}


