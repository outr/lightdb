package lightdb.index.feature

import lightdb.field.FieldFeature

sealed trait IndexFeature extends FieldFeature

object IndexFeature {
  case object Int extends IndexFeature

  case object String extends IndexFeature
}