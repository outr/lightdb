package lightdb.halo

import lightdb.{LightDB, Store}
import scribe.{Level, Logger}

trait HaloDBSupport {
  this: LightDB =>

  def indexThreads: Int = Runtime.getRuntime.availableProcessors()
  def maxFileSize: Int = 1024 * 1024

  Logger("com.oath.halodb").withMinimumLevel(Level.Warn).replace()

  override protected def createStore(name: String): Store =
    HaloDBStore(directory.resolve(name), indexThreads, maxFileSize)
}
