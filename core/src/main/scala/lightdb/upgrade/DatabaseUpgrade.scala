package lightdb.upgrade

import lightdb.LightDB
import rapid.Task

trait DatabaseUpgrade {
  def label: String = getClass.getSimpleName.replace("$", "")
  def applyToNew: Boolean
  def blockStartup: Boolean
  def alwaysRun: Boolean

  def upgrade(db: LightDB): Task[Unit]
}