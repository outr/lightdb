package lightdb.feature

case class DBFeatureKey[T](key: String) extends AnyVal with FeatureKey[T]