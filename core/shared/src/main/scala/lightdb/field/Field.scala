package lightdb.field

case class Field[T, F](name: String, getter: T => F, features: List[FieldFeature]) {
  def withFeature(feature: FieldFeature): Field[T, F] = copy(features = features ::: List(feature))
}