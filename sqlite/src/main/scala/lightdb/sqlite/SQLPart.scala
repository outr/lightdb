package lightdb.sqlite

trait SQLPart {
  def sql: String

  def args: List[Any]
}
