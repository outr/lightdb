package lightdb.transaction

case class SimpleTransaction[Doc]() extends Transaction[Doc] {
  override def commit(): Unit = {}

  override def rollback(): Unit = {}
}
