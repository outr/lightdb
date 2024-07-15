package lightdb.transaction

import lightdb.feature.FeatureKey

case class TransactionKey[T](key: String) extends AnyVal with FeatureKey[T]