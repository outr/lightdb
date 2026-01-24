package lightdb.traversal.store

import fabric.{Json, Null}
import lightdb.KeyValue
import lightdb.field.{Field, IndexingState}

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets

object TraversalIndex {
  private val Prefix = "trv:idx:"

  def prefixForStore(storeName: String): String = s"$Prefix${encode(storeName)}:"

  def prefixForField(storeName: String, fieldName: String): String =
    s"$Prefix${encode(storeName)}:${encode(fieldName)}:"

  def key(storeName: String, fieldName: String, value: String, docId: String): String =
    s"$Prefix${encode(storeName)}:${encode(fieldName)}:${encode(value)}:${encode(docId)}"

  def decodeDocId(key: String): Option[String] = {
    if !key.startsWith(Prefix) then return None
    val idx = key.lastIndexOf(':')
    if idx == -1 || idx == key.length - 1 then None
    else Some(decode(key.substring(idx + 1)))
  }

  def encodeValue(json: Json): String = json match {
    case Null => "null"
    case other => other.toString
  }

  def valuesForIndex[Doc <: lightdb.doc.Document[Doc]](field: Field[Doc, ?], doc: Doc, state: IndexingState): List[String] = {
    val raw = field.asInstanceOf[Field[Doc, Any]].get(doc, field.asInstanceOf[Field[Doc, Any]], state)
    valuesForIndexValue(raw).map {
      case null => "null"
      case s: String => s.toLowerCase
      case v => v.toString
    }
  }

  def valuesForIndexValue(value: Any): List[Any] = value match {
    case null => Nil
    case Some(v) => valuesForIndexValue(v)
    case None => List(null) // index null marker for None
    case it: Iterable[?] => it.asInstanceOf[Iterable[Any]].toList.flatMap(valuesForIndexValue)
    case arr: Array[?] => arr.toList.flatMap(valuesForIndexValue)
    case jc: java.util.Collection[?] => jc.toArray.toList.flatMap(valuesForIndexValue)
    case v => List(v)
  }

  private def encode(s: String): String = URLEncoder.encode(s, StandardCharsets.UTF_8)
  private def decode(s: String): String = URLDecoder.decode(s, StandardCharsets.UTF_8)
}


