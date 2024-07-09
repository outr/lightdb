package lightdb.error

case class ModelMissingFieldsException(collectionName: String, missingFields: List[String]) extends RuntimeException(s"The $collectionName DocModel does not include the field(s): ${missingFields.mkString(", ")}. Add the following to your DocModel:\n${missingFields.map(n => s"val $n = field(\"$n\", _.$n)").mkString("\n")}")
