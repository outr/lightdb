package lightdb.upgrade

import lightdb.LightDB

trait DatabaseUpgrade {
  def label: String = getClass.getSimpleName.replace("$", "")
  def applyToNew: Boolean
  def blockStartup: Boolean
  def alwaysRun: Boolean

  def upgrade(db: LightDB): Unit
}

object DatabaseUpgrade {

}