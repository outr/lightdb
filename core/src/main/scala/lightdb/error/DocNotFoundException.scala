package lightdb.error

case class DocNotFoundException[Doc](storeName: String,
                                     fieldName: String,
                                     value: Any) extends RuntimeException(s"$value not found for $fieldName in $storeName")
