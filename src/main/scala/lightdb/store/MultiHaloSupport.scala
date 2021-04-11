package lightdb.store

import lightdb.Document
import lightdb.collection.Collection

import java.nio.file.{Path, Paths}

trait MultiHaloSupport extends ObjectStoreSupport {
  private val defaultPath: Path = Paths.get("db")

  override def store[D <: Document[D]](collection: Collection[D]): ObjectStore = {
    val baseDir = collection.db.directory.getOrElse(defaultPath)
    HaloStore(baseDir.resolve(collection.collectionName).resolve("store"))
  }
}