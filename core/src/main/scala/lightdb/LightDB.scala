package lightdb

import java.nio.file.Path

abstract class LightDB(directory: Path,
                       indexThreads: Int = 2,
                       maxFileSize: Int = 1024 * 1024) {

  private var stores = List.empty[Store]

  protected[lightdb] def createStore(name: String): Store = synchronized {
    // TODO: verifyInitialized()
    val store = Store(directory.resolve(name), indexThreads, maxFileSize)
    stores = store :: stores
    store
  }
}
