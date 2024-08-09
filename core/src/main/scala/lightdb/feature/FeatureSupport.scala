package lightdb.feature

trait FeatureSupport[Key[T] <: FeatureKey[T]] {
  private var map = Map.empty[Key[Any], Any]

  def features: Iterable[Any] = map.values
  def featureMap: Map[Key[Any], Any] = map

  def put[T](key: Key[T], value: T): Unit = synchronized {
    map += key.asInstanceOf[Key[Any]] -> value
  }

  def get[T](key: Key[T]): Option[T] = map.get(key.asInstanceOf[Key[Any]])
    .map(_.asInstanceOf[T])

  def getOrCreate[T](key: Key[T], create: => T): T = synchronized {
    get[T](key) match {
      case Some(t) => t
      case None =>
        val t: T = create
        put(key, t)
        t
    }
  }

  def apply[T](key: Key[T]): T = get[T](key)
    .getOrElse(throw new RuntimeException(s"Key not found: $key. Keys: ${map.keys.mkString(", ")}"))
}