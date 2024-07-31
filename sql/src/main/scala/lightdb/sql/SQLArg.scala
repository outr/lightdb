package lightdb.sql

import fabric._
import fabric.define.DefType
import fabric.io.JsonFormatter
import fabric.rw._
import lightdb.spatial.GeoPoint
import lightdb.{Field, Id, Tokenized}

import java.sql.{JDBCType, PreparedStatement, SQLType, Types}

trait SQLArg {
  def set(ps: PreparedStatement, index: Int): Unit
}

object SQLArg {
  case class FieldArg[Doc, F](field: Field[Doc, F], value: F) extends SQLArg {
    private def setInternal(ps: PreparedStatement, index: Int, value: Any): Unit = value match {
      case null => ps.setNull(index, Types.NULL)
      case _ if field.isInstanceOf[Tokenized[_]] =>
        val s = value.toString
        ps.setString(index, s.toLowerCase.split("\\s+").filterNot(_.isEmpty).mkString(" "))
      case Some(value) => setInternal(ps, index, value)
      case None => ps.setNull(index, Types.NULL)
      case id: Id[_] => ps.setString(index, id.value)
      case s: String => ps.setString(index, s)
      case b: Boolean => ps.setInt(index, if (b) 1 else 0)
      case i: Int => ps.setInt(index, i)
      case l: Long => ps.setLong(index, l)
      case f: Float => ps.setFloat(index, f)
      case d: Double => ps.setDouble(index, d)
      case json: Json => ps.setString(index, JsonFormatter.Compact(json))
      case point: GeoPoint => ps.setString(index, s"POINT(${point.longitude} ${point.latitude})")
      case _ =>
        val json = value.asInstanceOf[F].json(field.rw)
        val string = if (field.rw.definition == DefType.Str) {
          json.asString
        } else {
          JsonFormatter.Compact(json)
        }
        ps.setString(index, string)
    }

    override def set(ps: PreparedStatement, index: Int): Unit = setInternal(ps, index, value)
  }

  object FieldArg {
    def apply[Doc, F](doc: Doc, field: Field[Doc, F]): FieldArg[Doc, F] = apply(field, field.get(doc))
  }

  case class StringArg(s: String) extends SQLArg {
    override def set(ps: PreparedStatement, index: Int): Unit = ps.setString(index, s)
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