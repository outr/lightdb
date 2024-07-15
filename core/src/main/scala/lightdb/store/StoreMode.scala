package lightdb.store

sealed trait StoreMode

object StoreMode {
  case object All extends StoreMode
  case object Indexes extends StoreMode
}