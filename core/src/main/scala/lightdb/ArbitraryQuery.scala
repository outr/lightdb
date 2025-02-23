package lightdb

import fabric.Json

case class ArbitraryQuery(query: String, params: Map[String, Json] = Map.empty) {
  def param(name: String, value: Json): ArbitraryQuery = copy(params = params + (name -> value))
  def params(tuples: (String, Json)*): ArbitraryQuery = copy(params = params ++ tuples.toMap)
}