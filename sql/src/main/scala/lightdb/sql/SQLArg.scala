package lightdb.sql

import fabric._
import fabric.define.DefType
import fabric.io.JsonFormatter
import fabric.rw._
import lightdb.spatial.GeoPoint
import lightdb.{Field, Id}

import java.sql.{PreparedStatement, Types}

trait SQLArg {
  def set(ps: PreparedStatement, index: Int): Unit
}

object SQLArg {
  case class FieldArg[Doc, F](field: Field[Doc, F], value: F) extends SQLArg {
    override def set(ps: PreparedStatement, index: Int): Unit = value match {
      case null => ps.setNull(index, Types.NULL)
      case id: Id[_] => ps.setString(index, id.value)
      case s: String => ps.setString(index, s)
      case b: Boolean => ps.setBoolean(index, b)
      case i: Int => ps.setInt(index, i)
      case l: Long => ps.setLong(index, l)
      case f: Float => ps.setFloat(index, f)
      case d: Double => ps.setDouble(index, d)
      case json: Json => ps.setString(index, JsonFormatter.Compact(json))
      case point: GeoPoint => ps.setString(index, s"POINT(${point.longitude} ${point.latitude})")
      case _ =>
        val json = value.json(field.rw)
        val string = if (field.rw.definition == DefType.Str) {
          json.asString
        } else {
          JsonFormatter.Compact(json)
        }
        ps.setString(index, string)
    }
  }

  object FieldArg {
    def apply[Doc, F](doc: Doc, field: Field[Doc, F]): FieldArg[Doc, F] = apply(field, field.get(doc))
  }

  case class LongArg(long: Long) extends SQLArg {
    override def set(ps: PreparedStatement, index: Int): Unit = ps.setLong(index, long)
  }

  case class DoubleArg(double: Double) extends SQLArg {
    override def set(ps: PreparedStatement, index: Int): Unit = ps.setDouble(index, double)
  }

  case class JsonArg(json: Json) extends SQLArg {
    override def set(ps: PreparedStatement, index: Int): Unit = ps.setString(index, JsonFormatter.Compact(json))
  }

  case class GeoPointArg(point: GeoPoint) extends SQLArg {
    override def set(ps: PreparedStatement, index: Int): Unit = ps.setString(index, s"POINT(${point.longitude} ${point.latitude})")
  }
}