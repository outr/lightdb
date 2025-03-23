package lightdb.util

final class ElasticHashMap[K, +V] private(private val levels: Vector[Vector[Option[(K, V)]]],
                                          private val numElements: Int,
                                          private val sizeThreshold: Int) extends Map[K, V] {
  private val loadFactor: Double = 0.75

  override def get(key: K): Option[V] = {
    val probeSeq = probeSequence(key)
    probeSeq.collectFirst {
      case (level, idx) if levels(level)(idx).exists(_._1 == key) => levels(level)(idx).get._2
    }
  }

  override def iterator: Iterator[(K, V)] =
    levels.flatten.flatten.iterator

  override def updated[V1 >: V](key: K, value: V1): ElasticHashMap[K, V1] = {
    val probeSeq = probeSequence(key)

    probeSeq.collectFirst {
      case (level, idx) if levels(level)(idx).forall(_._1 != key) =>
        val newLevels = levels.updated(
          level,
          levels(level).updated(idx, Some((key, value)))
        )
        val newCount = if (levels(level)(idx).isEmpty) numElements + 1 else numElements
        if (newCount > sizeThreshold) resize()
        else new ElasticHashMap(newLevels, newCount, sizeThreshold)
    }.getOrElse(this) // Return the existing instance if no insertion is possible
  }

  override def removed(key: K): ElasticHashMap[K, V] = {
    val probeSeq = probeSequence(key)

    probeSeq.collectFirst {
      case (level, idx) if levels(level)(idx).exists(_._1 == key) =>
        val newLevels = levels.updated(
          level,
          levels(level).updated(idx, None)
        )
        new ElasticHashMap(newLevels, numElements - 1, sizeThreshold)
    }.getOrElse(this) // Return the existing instance if key was not found
  }

  override def empty: ElasticHashMap[K, V] = ElasticHashMap.empty[K, V]()

  private def probeSequence(key: K): Seq[(Int, Int)] = {
    val hash = key.hashCode().abs
    for {
      level <- levels.indices
      idx = (hash + level * 31) % levels(level).length
    } yield (level, idx)
  }

  private def resize[V1 >: V](): ElasticHashMap[K, V1] = {
    val newCapacity = levels.head.length * 2
    val newMap = ElasticHashMap.empty[K, V1](newCapacity, loadFactor)
    this.iterator.foldLeft(newMap)((acc, entry) => acc.updated(entry._1, entry._2))
  }
}

object ElasticHashMap {
  def empty[K, V](initialCapacity: Int = 16, loadFactor: Double = 0.75): ElasticHashMap[K, V] = {
    val sizeThreshold = (initialCapacity * loadFactor).toInt
    val levels = Vector.tabulate(log2(initialCapacity) + 1)(i => Vector.fill(initialCapacity >> i)(None))
    new ElasticHashMap[K, V](levels, 0, sizeThreshold)
  }

  private def log2(x: Int): Int = (math.log(x) / math.log(2)).toInt
}