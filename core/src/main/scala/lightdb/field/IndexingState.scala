package lightdb.field

class IndexingState {
  private var map = Map.empty[IndexingKey[_], Any]

  def get[T](key: IndexingKey[T]): Option[T] = map.get(key).map(_.asInstanceOf[T])
  def getOrCreate[T](key: IndexingKey[T], create: => T): T = synchronized {
    get(key) match {
      case Some(value) => value
      case None =>
        val value: T = create
        set(key, value)
        value
    }
  }
  def apply[T](key: IndexingKey[T]): T = get[T](key).getOrElse(throw new NullPointerException(s"Not found: $key"))
  def set[T](key: IndexingKey[T], value: T): Unit = synchronized {
    map += key -> value
  }
  def remove[T](key: IndexingKey[T]): Unit = synchronized {
    map -= key
  }
  def clear(): Unit = synchronized {
    map = Map.empty
  }
}