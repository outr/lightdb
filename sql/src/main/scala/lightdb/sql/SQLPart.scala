package lightdb.sql

import fabric.Json

case class SQLPart(sql: String, args: List[Json])
