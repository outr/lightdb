package lightdb.field

import lightdb.{Document, ObjectMapping}

case class Field[D <: Document[D], F](name: String,
                                      getter: D => F,
                                      features: List[FieldFeature],
                                      mapping: ObjectMapping[D]) {
  def withFeature(feature: FieldFeature): Field[D, F] = {
    mapping.field.replace(copy(features = features ::: List(feature)))
  }
}