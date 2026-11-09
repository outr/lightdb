package lightdb.postgresql

import fabric.*

/**
 * Text as PostgreSQL can store it. Its text and jsonb types refuse a NUL character (jsonb its escaped form too), and its UTF-8
 * encoding has no form for a UTF-16 surrogate without its pair, so each becomes U+FFFD. A surrogate pair is kept.
 */
object PostgreSQLText {
  val Replacement: Char = 0xfffd.toChar

  private def invalidAt(s: String, i: Int): Boolean = {
    val c = s.charAt(i)
    if c == '\u0000' then true
    else if Character.isHighSurrogate(c) then i + 1 >= s.length || !Character.isLowSurrogate(s.charAt(i + 1))
    else if Character.isLowSurrogate(c) then i == 0 || !Character.isHighSurrogate(s.charAt(i - 1))
    else false
  }

  private def firstInvalid(s: String): Int = {
    var i = 0
    while i < s.length && !invalidAt(s, i) do i += 1
    if i < s.length then i else -1
  }

  /** `s` with each NUL and each unpaired surrogate replaced with U+FFFD; `s` itself when it holds neither. */
  def clean(s: String): String = firstInvalid(s) match {
    case -1 => s
    case first =>
      val b = new java.lang.StringBuilder(s.length)
      b.append(s, 0, first)
      var i = first
      while i < s.length do {
        b.append(if invalidAt(s, i) then Replacement else s.charAt(i))
        i += 1
      }
      b.toString
  }

  /** `json` with every string in it, object keys included, cleaned; `json` itself when none changes. */
  def clean(json: Json): Json = if valid(json) then json else rebuild(json)

  private def valid(json: Json): Boolean = json match {
    case s: Str => firstInvalid(s.value) == -1
    case a: Arr => a.value.forall(valid)
    case o: Obj => o.value.forall((key, value) => firstInvalid(key) == -1 && valid(value))
    case _ => true
  }

  private def rebuild(json: Json): Json = json match {
    case s: Str => s.copy(value = clean(s.value))
    case a: Arr => a.copy(value = a.value.map(rebuild))
    case o: Obj =>
      val value = o.value.map((key, value) => clean(key) -> rebuild(value))
      o.reference.fold(Obj(value))(Obj(value, _))
    case other => other
  }
}
