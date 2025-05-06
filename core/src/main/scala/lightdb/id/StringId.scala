package lightdb.id

case class StringId[Doc](value: String) extends AnyVal with Id[Doc]