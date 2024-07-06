package lightdb.sql

case class SQLPart(sql: String, args: List[SQLArg] = Nil)