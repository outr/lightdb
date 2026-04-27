package lightdb.query

/**
 * Per-hit highlighting result. `byField` maps a field name to its highlighted fragments
 * (already wrapped in the configured pre/post tags by the backend).
 *
 * Empty when no highlight was requested or the backend returned none.
 */
case class Highlights(byField: Map[String, List[String]] = Map.empty) {
  def isEmpty: Boolean = byField.isEmpty
  def nonEmpty: Boolean = byField.nonEmpty
  def get(field: String): List[String] = byField.getOrElse(field, Nil)
}

object Highlights {
  val empty: Highlights = Highlights()
}
