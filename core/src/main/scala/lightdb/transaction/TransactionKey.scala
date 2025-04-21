package lightdb.transaction

import lightdb.feature.FeatureKey
import rapid.Unique

case class TransactionKey[T](key: String = Unique()) extends AnyVal with FeatureKey[T]