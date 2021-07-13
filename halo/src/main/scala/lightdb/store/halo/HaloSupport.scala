package lightdb.store.halo

import lightdb.store.ObjectStoreSupport

import java.nio.file.Path

trait HaloSupport extends ObjectStoreSupport {
  protected def haloIndexThreads: Int = 2

  protected def haloMaxFileSize: Int = 1024 * 1024

  protected def createStore(directory: Path): HaloStore = HaloStore(
    directory = directory,
    indexThreads = haloIndexThreads,
    maxFileSize = haloMaxFileSize
  )
}