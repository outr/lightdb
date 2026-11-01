package lightdb.duckdb

/**
 * A CSV column (by its heading, or by its place, counting from 1) and the field of the store it goes to. The same column may go to
 * more than one field: a text field and, beside it, a numeric one for sorting by value (`tryCast = Some("DOUBLE")`: a value that is not
 * a number (a blank, a word) becomes NULL there, where a plain cast would fail the whole import).
 */
final case class CsvColumn(source: CsvColumn.Source, field: String, tryCast: Option[String] = None)

object CsvColumn {
  enum Source {
    case Heading(name: String)
    case Place(number: Int)
  }

  def byHeading(heading: String, field: String, tryCast: Option[String] = None): CsvColumn = CsvColumn(Source.Heading(heading), field, tryCast)
  def byPlace(number: Int, field: String, tryCast: Option[String] = None): CsvColumn = CsvColumn(Source.Place(number), field, tryCast)
}
