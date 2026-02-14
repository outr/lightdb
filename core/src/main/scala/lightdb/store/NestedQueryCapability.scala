package lightdb.store

sealed trait NestedQueryCapability {
  def supportsNestedQueries: Boolean
  def isNative: Boolean
}

object NestedQueryCapability {
  case object Unsupported extends NestedQueryCapability {
    override val supportsNestedQueries: Boolean = false
    override val isNative: Boolean = false
  }

  case object Fallback extends NestedQueryCapability {
    override val supportsNestedQueries: Boolean = true
    override val isNative: Boolean = false
  }

  case object Native extends NestedQueryCapability {
    override val supportsNestedQueries: Boolean = true
    override val isNative: Boolean = true
  }
}
