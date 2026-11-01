package lightdb.sql

import java.util.Locale

/**
 * The words of a tokenized field as the SQL stores match and rank them: lower-cased runs of letters, digits and underscores
 * (Lucene's standard tokenizer also keeps `read_file` one word). A text is matched in its word form: each word between two
 * spaces, every run of other characters two spaces, so a word or a run of consecutive words is found by plain position and
 * occurrences do not share their spaces (`" read  file "` holds `" read "` once and `" read  file "` once, and `" thread "` holds
 * neither). A query token is the words it holds, matched as consecutive words; one with no words matches nothing.
 */
object TextWords {
  /** A run of characters that are not part of a word, as a Java (and RE2, PCRE) regular expression. */
  val Separator: String = "[^\\p{L}\\p{N}_]+"

  def apply(token: String): List[String] = token.toLowerCase(Locale.ROOT).split(Separator).toList.filter(_.nonEmpty)

  /** What a text's word form holds where it has `words` in a row. */
  def pattern(words: List[String]): String = words.mkString(" ", "  ", " ")

  /** A text's word form. */
  def form(text: String): String = " " + text.toLowerCase(Locale.ROOT).replaceAll(Separator, "  ") + " "

  /** A LIKE pattern for text holding `word` anywhere, with LIKE's own characters escaped by backslash. */
  def containsLike(word: String): String =
    "%" + word.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"

  /** The k1 and b of BM25, as Lucene's defaults. */
  val K1: Double = 1.2
  val B: Double = 0.75
}
