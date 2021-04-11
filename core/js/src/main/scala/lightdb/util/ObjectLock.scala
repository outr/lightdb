package lightdb.util

object ObjectLock extends AbstractObjectLock {
  override def isEmpty: Boolean = true

  override def apply[Key](key: Key, f: ObjectLock.ReleasableLock => Unit): Unit = f(DoNothingReleasableLock)

  override protected def triggerNext[Key](key: Key): Unit = {}

  object DoNothingReleasableLock extends ReleasableLock {
    override def release(): Unit = {}
  }
}