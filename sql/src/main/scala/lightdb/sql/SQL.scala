package lightdb.sql

trait SQL {
  def sql: String

  def args: List[SQLArg]
}
