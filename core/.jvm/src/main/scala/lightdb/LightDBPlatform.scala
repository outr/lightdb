package lightdb

import rapid.Task

import java.nio.file.Path

/** JVM implementations of the few behaviors that differ between the JVM and Scala.js builds of core. */
private[lightdb] object LightDBPlatform {
  /** Where a store named `name` keeps its files under the database directory. */
  def storePath(directory: Option[Path], name: String): Option[Path] = directory.map(_.resolve(name))

  /** Run `task` when the JVM shuts down. */
  def onShutdown(task: => Task[Unit]): Unit = Runtime.getRuntime.addShutdownHook(new Thread(() => {
    task.sync()
  }))

  /** Run `task` so that no other `exclusive` call on the same `lock` overlaps it. */
  def exclusive[T](lock: AnyRef)(task: => Task[T]): Task[T] = Task {
    lock.synchronized {
      task.sync()
    }
  }
}
