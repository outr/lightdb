package lightdb.util

/**
 * Based on the Elastic Hash Table defined here: https://github.com/MWARDUNI/ElasticHashing
 */
final class ElasticHashMap[K, +V] private (private val levels: Vector[Vector[Option[(K, V)]]],
                                           override val size: Int,
                                           private val sizeThreshold: Int) extends Map[K, V] {
  private val loadFactor: Double = 0.75

  override def get(key: K): Option[V] = {
    val probeSeq = probeSequence(key)
    probeSeq.collectFirst {
      case (level, idx) if levels(level)(idx).exists(_._1 == key) =>
        levels(level)(idx).get._2
    }
  }

  override def iterator: Iterator[(K, V)] =
    levels.flatten.flatten.iterator

  override def updated[V1 >: V](key: K, value: V1): ElasticHashMap[K, V1] = {
    val probeSeq = probeSequence(key)

    probeSeq.collectFirst {
      case (level, idx) =>
        val currentSlot = levels(level)(idx)
        currentSlot match {
          case Some((existingKey, _)) if existingKey == key =>
            // Key exists, update in-place without increasing size
            val newLevels = levels.updated(level, levels(level).updated(idx, Some((key, value))))
            new ElasticHashMap(newLevels, size, sizeThreshold)

          case None =>
            // Slot is empty, inserting new key
            val newLevels = levels.updated(level, levels(level).updated(idx, Some((key, value))))
            val newSize = size + 1
            if (newSize > sizeThreshold) resize().updated(key, value) // Retry insertion after resize
            else new ElasticHashMap(newLevels, newSize, sizeThreshold)

          case _ => // Slot is occupied by a different key, continue probing
            null
        }
    }.filter(_ != null).getOrElse(this)
  }

  override def removed(key: K): ElasticHashMap[K, V] = {
    val probeSeq = probeSequence(key)

    probeSeq.collectFirst {
      case (level, idx) if levels(level)(idx).exists(_._1 == key) =>
        val newLevels = levels.updated(level, levels(level).updated(idx, None))
        new ElasticHashMap(newLevels, size - 1, sizeThreshold)
    }.getOrElse(this)
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
    this.iterator.foldLeft(newMap) { case (acc, (k, v)) => acc.updated(k, v) }
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