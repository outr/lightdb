package lightdb.id

import perfolation.*

/**
 * An Id backed by a Long counter, zero-padded to 19 digits so string ordering matches numeric ordering.
 * Allocated by `IncrementingIdAllocator` — do not construct directly unless you know what you're doing.
 */
case class IncrementingId[Doc](number: Long) extends AnyVal with Id[Doc] {
  override def value: String = IncrementingId.format(number)
}

object IncrementingId {
  /** Zero-padded to Long.MaxValue width (19 digits) so lexicographic = numeric order. */
  def format(n: Long): String = n.f(i = 19)

  /** Parse an id value back into a Long. Returns None if the value is not a valid incrementing id. */
  def parse(value: String): Option[Long] =
    if value.length == 19 && value.forall(_.isDigit) then value.toLongOption else None
}
