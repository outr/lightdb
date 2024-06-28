package lightdb.document

sealed trait SetType

object SetType {
  case object Insert extends SetType
  case object Replace extends SetType
  case object Unknown extends SetType
}