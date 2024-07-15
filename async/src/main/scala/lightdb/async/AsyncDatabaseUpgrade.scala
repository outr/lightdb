package lightdb.async

import cats.effect.IO

trait AsyncDatabaseUpgrade {
  def label: String = getClass.getSimpleName.replace("$", "")
  def applyToNew: Boolean
  def blockStartup: Boolean
  def alwaysRun: Boolean

  def upgrade(db: AsyncLightDB): IO[Unit]
}