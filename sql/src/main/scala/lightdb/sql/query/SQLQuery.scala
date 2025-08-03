package lightdb.sql.query

import fabric.Json

import java.nio.file.{Files, Path}
import scala.collection.mutable

case class SQLQuery(parts: List[SQLPart]) extends SQLPart {
  lazy val flatParts: List[SQLPart] = parts.flatMap {
    case subQuery: SQLQuery => subQuery.flatParts
    case part => List(part)
  }

  lazy val query: String = flatParts.collect {
    case SQLPart.Fragment(value) => value
    case SQLPart.Placeholder(_) => "?"
    case SQLPart.Arg(_) => "?"
  }.mkString

  lazy val bindMap: Map[String, Json] = flatParts.collect {
    case SQLPart.Bind(name, value) => name -> value
  }.toMap

  lazy val missingBinds: List[String] = flatParts.collect {
    case SQLPart.Placeholder(Some(name)) if !bindMap.contains(name) => name
    case SQLPart.Placeholder(None) => "?"
  }

  lazy val hasUnboundPositional: Boolean = flatParts.exists {
    case SQLPart.Placeholder(None) => true
    case _ => false
  }

  lazy val args: List[Json] = flatParts.collect {
    case SQLPart.Placeholder(Some(name)) =>
      bindMap.getOrElse(name, throw new RuntimeException(s"Found placeholder for '$name' arg, but no binding is found."))
    case SQLPart.Placeholder(None) =>
      throw new RuntimeException(s"Found unnamed placeholder without binding!")
    case SQLPart.Arg(json) => json
  }

  def withParts(parts: SQLPart*): SQLQuery = copy(parts = this.parts ::: parts.toList)
  def fragment(sql: String): SQLQuery = withParts(SQLPart.Fragment(sql))
  def placeholder(): SQLQuery = withParts(SQLPart.Placeholder(None))
  def placeholder(name: String): SQLQuery = withParts(SQLPart.Placeholder(Some(name)))
  def bind(name: String, value: Json): SQLQuery = withParts(SQLPart.Bind(name, value))
  def arg(value: Json): SQLQuery = withParts(SQLPart.Arg(value))

  def fillPlaceholder(value: Json): SQLQuery = {
    var found = false
    val updated = parts.map {
      case SQLPart.Placeholder(None) if !found =>
        found = true
        SQLPart.Arg(value)
      case part => part
    }
    if (!found)
      throw new RuntimeException("No unnamed placeholder found to fill!")
    SQLQuery(updated)
  }

  def fillPlaceholder(name: String, value: Json): SQLQuery = {
    var found = false
    val updated = parts.map {
      case SQLPart.Placeholder(Some(n)) if n == name =>
        found = true
        SQLPart.Arg(value)
      case part => part
    }
    if (!found)
      throw new RuntimeException(s"No placeholders found for named bind '$name'")
    SQLQuery(updated)
  }
}

object SQLQuery {
  private val PlaceholderPattern =
    raw"(:[a-zA-Z_][a-zA-Z0-9_]*)|\?".r // matches either named (e.g. :name) or positional (?)

  def load(path: Path): SQLQuery = parse(Files.readString(path))

  def parse(sql: String): SQLQuery = {
    val parts = mutable.ListBuffer.empty[SQLPart]
    var lastIndex = 0

    for (m <- PlaceholderPattern.findAllMatchIn(sql)) {
      if (m.start > lastIndex) {
        val before = sql.substring(lastIndex, m.start)
        parts += SQLPart.Fragment(before)
      }

      val matched = m.group(0)
      val placeholder = if (matched.startsWith(":")) {
        SQLPart.Placeholder(Some(matched.drop(1)))
      } else {
        SQLPart.Placeholder(None)
      }

      parts += placeholder
      lastIndex = m.end
    }

    if (lastIndex < sql.length) {
      parts += SQLPart.Fragment(sql.substring(lastIndex))
    }

    SQLQuery(parts.toList)
  }
}

sealed trait SQLPart

object SQLPart {
  case class Fragment(value: String) extends SQLPart
  case class Placeholder(name: Option[String]) extends SQLPart
  case class Bind(name: String, value: Json) extends SQLPart
  case class Arg(value: Json) extends SQLPart
}