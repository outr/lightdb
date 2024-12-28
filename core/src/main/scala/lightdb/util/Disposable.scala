package lightdb.util

import rapid.Task

trait Disposable {
  def dispose(): Task[Unit]
}
