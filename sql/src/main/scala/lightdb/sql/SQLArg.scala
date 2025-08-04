package lightdb.sql

import fabric._
import fabric.define.DefType
import fabric.io.JsonFormatter
import fabric.rw._
import lightdb.doc.Document
import lightdb.field.Field._
import lightdb.field.{Field, IndexingState}
import lightdb.id.Id
import lightdb.spatial.Geo

import java.sql.{PreparedStatement, Types}

trait SQLArg {
//  def set(ps: PreparedStatement, index: Int): Unit

  def json: Json
}

object SQLArg {
  case class FieldArg[Doc <: Document[Doc], F](field: Field[Doc, F], value: F) extends SQLArg {
    private def jsonInternal(value: Any): Json = {
      scribe.trace(s"SQLArg: $value (${if (value != null) value.getClass.getName else "null"})")
      value match {
        case null | None => Null
        case _ if field.isInstanceOf[Tokenized[_]] =>
          val s = value.toString
          str(s.toLowerCase.split("\\s+").filterNot(_.isEmpty).mkString(" "))
        case Some(value) => jsonInternal(value)
        case id: Id[_] => str(id.value)
        case s: String => str(s)
        case b: Boolean => num(if (b) 1 else 0)
        case i: Int => num(i)
        case l: Long => num(l)
        case f: Float => num(f)
        case d: Double => num(d)
        case bd: BigDecimal => num(bd.toDouble)
        case json: Json => str(JsonFormatter.Compact(json))
//        case point: Geo.Point => ps.setString(index, s"POINT(${point.longitude} ${point.latitude})")
        case _ =>
          val json = if (field.rw.definition.isOpt) {
            Some(value).asInstanceOf[F].json(field.rw)
          } else {
            value.asInstanceOf[F].json(field.rw)
          }
          val string = if (field.rw.definition == DefType.Str) {
            json.asString
          } else {
            JsonFormatter.Compact(json)
          }
          str(string)
      }
    }

    override def json: Json = jsonInternal(value)

    //    override def set(ps: PreparedStatement, index: Int): Unit = setInternal(ps, index, value)

    override def toString: String = s"FieldArg(field = ${field.name}, value = $value (${value.getClass.getName}))"
  }

  object FieldArg {
    def apply[Doc <: Document[Doc], F](doc: Doc,
                                       field: Field[Doc, F],
                                       state: IndexingState): FieldArg[Doc, F] = apply(field, field.get(doc, field, state))
  }

  case class StringArg(s: String) extends SQLArg {
//    override def set(ps: PreparedStatement, index: Int): Unit = ps.setString(index, s)

    override def json: Json = str(s)
  }

  case class LongArg(long: Long) extends SQLArg {
//    override def set(ps: PreparedStatement, index: Int): Unit = ps.setLong(index, long)

    override def json: Json = num(long)
  }

  case class DoubleArg(double: Double) extends SQLArg {
//    override def set(ps: PreparedStatement, index: Int): Unit = ps.setDouble(index, double)

    override def json: Json = num(double)
  }

  case class JsonArg(json: Json) extends SQLArg {
//    override def set(ps: PreparedStatement, index: Int): Unit = ps.setString(index, JsonFormatter.Compact(json))
  }

  case class GeoPointArg(point: Geo.Point) extends SQLArg {
//    override def set(ps: PreparedStatement, index: Int): Unit = ps.setString(index, s"POINT(${point.longitude} ${point.latitude})")

    override def json: Json = str(s"POINT(${point.longitude} ${point.latitude})")
  }
}