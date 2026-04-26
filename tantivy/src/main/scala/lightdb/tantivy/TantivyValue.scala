package lightdb.tantivy

import com.google.protobuf.ByteString
import fabric.*
import fabric.io.JsonFormatter
import fabric.rw.RW
import lightdb.facet.FacetValue
import lightdb.field.Field
import scantivy.proto as pb

/** Conversions between LightDB Field values and scantivy `pb.Value`. */
object TantivyValue {

  def fromJson(field: Field[?, ?], json: Json): Option[pb.Value] = json match {
    case Null => None
    case Str(s, _) => Some(string(s))
    case NumInt(n, _) => Some(long(n))
    case NumDec(n, _) => Some(double(n.toDouble))
    case Bool(b, _) => Some(bool(b))
    case _: Arr | _: Obj => Some(jsonValue(json))
  }

  def fromAny(field: Field[?, ?], v: Any): pb.Value = v match {
    case null => pb.Value(pb.Value.Kind.StringValue(""))
    case None => pb.Value(pb.Value.Kind.StringValue(""))
    case Some(inner) => fromAny(field, inner)
    case s: String => string(s)
    case b: Boolean => bool(b)
    case i: Int => long(i.toLong)
    case l: Long => long(l)
    case d: Double => double(d)
    case f: Float => double(f.toDouble)
    case fv: FacetValue => facet(fv)
    case id: lightdb.id.Id[?] => string(id.value)
    case other =>
      // Fallback via fabric: encode through the field's RW. Wrap to swallow ClassCastExceptions
      // when the value type doesn't match the field's RW (common when iterating element-by-element
      // on a List/Set field whose RW is for the collection, not the element).
      val json = try field.rw.asInstanceOf[RW[Any]].read(other)
      catch case _: ClassCastException => fabric.Str(other.toString)
      fromJson(field, json).getOrElse(string(json.toString))
  }

  // ---- builders ---------------------------------------------------------------------------

  def string(s: String): pb.Value = pb.Value(pb.Value.Kind.StringValue(s))
  def long(n: Long): pb.Value = pb.Value(pb.Value.Kind.LongValue(n))
  def double(n: Double): pb.Value = pb.Value(pb.Value.Kind.DoubleValue(n))
  def bool(b: Boolean): pb.Value = pb.Value(pb.Value.Kind.BoolValue(b))
  def facet(fv: FacetValue): pb.Value =
    pb.Value(pb.Value.Kind.FacetValue("/" + fv.path.mkString("/")))
  def jsonValue(json: Json): pb.Value =
    pb.Value(pb.Value.Kind.JsonValue(pb.JsonValue(encoded = ByteString.copyFrom(JsonFormatter.Compact(json).getBytes("UTF-8")))))

  // ---- decoding back into Json -------------------------------------------------------------

  def toJson(v: pb.Value): Json = v.kind match {
    case pb.Value.Kind.StringValue(s) => Str(s)
    case pb.Value.Kind.LongValue(n) => NumInt(n)
    case pb.Value.Kind.UlongValue(n) => NumInt(n)
    case pb.Value.Kind.DoubleValue(n) => NumDec(BigDecimal(n))
    case pb.Value.Kind.BoolValue(b) => Bool(b)
    case pb.Value.Kind.FacetValue(s) => Str(s)
    case pb.Value.Kind.DateMillis(ms) => NumInt(ms)
    case pb.Value.Kind.BytesValue(b) => Str(b.toByteArray.map("%02x".format(_)).mkString)
    case pb.Value.Kind.IpValue(s) => Str(s)
    case pb.Value.Kind.JsonValue(jv) =>
      fabric.io.JsonParser(new String(jv.encoded.toByteArray, "UTF-8"))
    case pb.Value.Kind.Empty => Null
  }
}
