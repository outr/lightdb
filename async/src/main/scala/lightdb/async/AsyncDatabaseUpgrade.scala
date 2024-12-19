package lightdb.async

import rapid.Task

trait AsyncDatabaseUpgrade {
  def label: String = getClass.getSimpleName.replace("$", "")
  def applyToNew: Boolean
  def blockStartup: Boolean
  def alwaysRun: Boolean

  def upgrade(db: AsyncLightDB): Task[Unit]
}