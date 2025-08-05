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
    override def json: Json = field.rw.read(value)

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