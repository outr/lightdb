package lightdb.error

case class ModelMissingFieldsException(storeName: String, missingFields: List[String]) extends RuntimeException(
  s"The $storeName DocModel does not include the field(s): ${missingFields.mkString(", ")}. " +
  "Add these fields to your DocModel."
)
