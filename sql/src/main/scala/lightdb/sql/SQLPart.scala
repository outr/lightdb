package lightdb.sql

case class SQLPart(sql: String, args: List[SQLArg] = Nil) extends SQL

object SQLPart {
  def merge(parts: SQLPart*): SQLPart = SQLPart(
    sql = parts.map(_.sql).mkString(" AND "),
    args = parts.toList.flatMap(_.args)
  )
}