package lightdb.error

case class DocNotFoundException[Doc](collectionName: String,
                                     fieldName: String,
                                     value: Any) extends RuntimeException(s"$value not found for $fieldName in $collectionName")
