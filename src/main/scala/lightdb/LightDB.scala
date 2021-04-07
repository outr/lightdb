package lightdb

import cats.effect.IO
import lightdb.store.ObjectStore

class LightDB(val store: ObjectStore) {
  def dispose(): IO[Unit] = store.dispose()
}