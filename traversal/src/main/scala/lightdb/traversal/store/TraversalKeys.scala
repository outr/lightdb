package lightdb.traversal.store

import fabric.io.JsonFormatter
import fabric.{Arr, Bool, Json, Null, NumDec, NumInt, Obj, Str}

object TraversalKeys {
  // Persisted traversal indexes are stored in a dedicated KeyValue store (indexBacking) and namespaced per collection.
  //
  // IMPORTANT: keys must be stable and prefix-friendly for RocksDBTransaction.jsonPrefixStream, which uses UTF-8 string keys.

  def metaReadyKey(storeName: String): String = s"ti:$storeName:meta:ready"

  // Per-doc reference mapping for Id-valued fields (single-valued only):
  // - ti:<store>:ref:<field>:<docId> -> "<refId>"
  //
  // This is used to map child -> parent ids for ExistsChild-style semi-joins without loading the child document.
  def refKey(storeName: String, field: String, docId: String): String =
    s"ti:$storeName:ref:$field:$docId"

  // Equality postings:
  // - keys must be <= 128 bytes (Id constraint), so we store hashes in the key and the actual docId in the JSON value.
  // - ti:<store>:eq:<field>:<valueHash>:<docHash> -> "<docId>"
  def eqKey(storeName: String, field: String, encodedValue: String, docId: String): String =
    s"ti:$storeName:eq:$field:${hashPart(encodedValue)}:${hashPart(docId)}"
  def eqPrefix(storeName: String, field: String, encodedValue: String): String =
    s"ti:$storeName:eq:$field:${hashPart(encodedValue)}:"

  // Equality postings (ordered by docId):
  // - ti:<store>:eqo:<field>:<valueHash>:<docId> -> "<docId>"
  // This enables streaming in Sort.IndexOrder without materializing huge seed sets.
  def eqoKey(storeName: String, field: String, encodedValue: String, docId: String): String =
    s"ti:$storeName:eqo:$field:${hashPart(encodedValue)}:$docId"
  def eqoPrefix(storeName: String, field: String, encodedValue: String): String =
    s"ti:$storeName:eqo:$field:${hashPart(encodedValue)}:"

  // N-gram postings (trigrams):
  // - ti:<store>:ng:<field>:<gram>:<docHash> -> "<docId>"
  def ngKey(storeName: String, field: String, gram: String, docId: String): String =
    s"ti:$storeName:ng:$field:$gram:${hashPart(docId)}"
  def ngPrefix(storeName: String, field: String, gram: String): String =
    s"ti:$storeName:ng:$field:$gram:"

  // Token postings (for tokenized fields):
  // - ti:<store>:tok:<field>:<token>:<docHash> -> "<docId>"
  // Tokenization is whitespace-splitting, lowercased (matches default SQL tokenizedEqualsPart semantics).
  def tokKey(storeName: String, field: String, token: String, docId: String): String =
    s"ti:$storeName:tok:$field:$token:${hashPart(docId)}"
  def tokPrefix(storeName: String, field: String, token: String): String =
    s"ti:$storeName:tok:$field:$token:"

  // Token postings ordered by docId (for Sort.IndexOrder streaming seeds when safe):
  // - ti:<store>:toko:<field>:<token>:<docId> -> "<docId>"
  def tokoKey(storeName: String, field: String, token: String, docId: String): String =
    s"ti:$storeName:toko:$field:$token:$docId"
  def tokoPrefix(storeName: String, field: String, token: String): String =
    s"ti:$storeName:toko:$field:$token:"

  // StartsWith postings (bounded prefixes):
  // - ti:<store>:sw:<field>:<prefix>:<docHash> -> "<docId>"
  // Note: <prefix> is a (possibly truncated) query/value prefix to keep key length bounded. Query execution must
  // still verify startsWith against the full stored value.
  def swKey(storeName: String, field: String, prefix: String, docId: String): String =
    s"ti:$storeName:sw:$field:$prefix:${hashPart(docId)}"
  def swPrefix(storeName: String, field: String, prefix: String): String =
    s"ti:$storeName:sw:$field:$prefix:"

  // StartsWith postings ordered by docId:
  // - ti:<store>:swo:<field>:<valuePrefix>:<docId> -> "<docId>"
  // Where <valuePrefix> is the first `prefixMaxLen` chars of the (lowercased) indexed value.
  // Query execution can prefix-scan using a shorter query prefix (no need to store 1..N prefixes per doc).
  def swoKey(storeName: String, field: String, valuePrefix: String, docId: String): String =
    s"ti:$storeName:swo:$field:$valuePrefix:$docId"
  // NOTE: intentionally NO trailing ':' so shorter query prefixes match longer valuePrefix segments.
  def swoPrefix(storeName: String, field: String, queryPrefix: String): String =
    s"ti:$storeName:swo:$field:$queryPrefix"

  // EndsWith postings (bounded reverse-prefixes):
  // - ti:<store>:ew:<field>:<revPrefix>:<docHash> -> "<docId>"
  // Where <revPrefix> is a prefix of the *reversed* lowercased value. Query execution must still verify endsWith.
  def ewKey(storeName: String, field: String, revPrefix: String, docId: String): String =
    s"ti:$storeName:ew:$field:$revPrefix:${hashPart(docId)}"
  def ewPrefix(storeName: String, field: String, revPrefix: String): String =
    s"ti:$storeName:ew:$field:$revPrefix:"

  // EndsWith postings ordered by docId:
  // - ti:<store>:ewo:<field>:<revValuePrefix>:<docId> -> "<docId>"
  // Where <revValuePrefix> is the first `prefixMaxLen` chars of the reversed lowercased indexed value.
  def ewoKey(storeName: String, field: String, revValuePrefix: String, docId: String): String =
    s"ti:$storeName:ewo:$field:$revValuePrefix:$docId"
  // NOTE: intentionally NO trailing ':' so shorter query prefixes match longer revValuePrefix segments.
  def ewoPrefix(storeName: String, field: String, queryRevPrefix: String): String =
    s"ti:$storeName:ewo:$field:$queryRevPrefix"

  // Range postings (numeric):
  // - ti:<store>:rl:<field>:<hexPrefix>:<docHash> -> "<docId>"  (sortable long encoding)
  // - ti:<store>:rd:<field>:<hexPrefix>:<docHash> -> "<docId>"  (sortable double encoding)
  //
  // hexPrefix is a prefix of a fixed-width 16-hex-digit sortable encoding; postings are stored for multiple
  // prefix lengths to allow range queries to be decomposed into a bounded set of prefix scans.
  def rlKey(storeName: String, field: String, hexPrefix: String, docId: String): String =
    s"ti:$storeName:rl:$field:$hexPrefix:${hashPart(docId)}"
  def rlPrefix(storeName: String, field: String, hexPrefix: String): String =
    s"ti:$storeName:rl:$field:$hexPrefix:"

  def rdKey(storeName: String, field: String, hexPrefix: String, docId: String): String =
    s"ti:$storeName:rd:$field:$hexPrefix:${hashPart(docId)}"
  def rdPrefix(storeName: String, field: String, hexPrefix: String): String =
    s"ti:$storeName:rd:$field:$hexPrefix:"

  // Range postings ordered by docId (for Sort.IndexOrder streaming seeds):
  // - ti:<store>:rlo:<field>:<hexPrefixFixedLen>:<docId> -> "<docId>"
  // - ti:<store>:rdo:<field>:<hexPrefixFixedLen>:<docId> -> "<docId>"
  //
  // NOTE: Unlike rl/rd (which store 1..NumericPrefixMaxLen prefixes per doc), these ordered postings store ONLY
  // ONE fixed-length prefix per numeric value, to avoid write amplification.
  def rloKey(storeName: String, field: String, hexPrefixFixedLen: String, docId: String): String =
    s"ti:$storeName:rlo:$field:$hexPrefixFixedLen:$docId"
  def rloPrefix(storeName: String, field: String, hexPrefixFixedLen: String): String =
    s"ti:$storeName:rlo:$field:$hexPrefixFixedLen:"

  def rdoKey(storeName: String, field: String, hexPrefixFixedLen: String, docId: String): String =
    s"ti:$storeName:rdo:$field:$hexPrefixFixedLen:$docId"
  def rdoPrefix(storeName: String, field: String, hexPrefixFixedLen: String): String =
    s"ti:$storeName:rdo:$field:$hexPrefixFixedLen:"

  // Ordered-by-field numeric postings (for Sort.ByField streaming pages):
  // - ti:<store>:ola:<field>:<hex16>:<docId> -> "<docId>"  (ascending sortable long)
  // - ti:<store>:old:<field>:<hex16desc>:<docId> -> "<docId>" (descending sortable long)
  // - ti:<store>:oda:<field>:<hex16>:<docId> -> "<docId>"  (ascending sortable double)
  // - ti:<store>:odd:<field>:<hex16desc>:<docId> -> "<docId>" (descending sortable double)
  //
  // NOTE: docId is included to keep ordering deterministic for equal values. Keys are guarded by safeId() at write-time.
  def olaKey(storeName: String, field: String, hex16: String, docId: String): String =
    s"ti:$storeName:ola:$field:$hex16:$docId"
  def olaPrefix(storeName: String, field: String): String =
    s"ti:$storeName:ola:$field:"

  def oldKey(storeName: String, field: String, hex16Desc: String, docId: String): String =
    s"ti:$storeName:old:$field:$hex16Desc:$docId"
  def oldPrefix(storeName: String, field: String): String =
    s"ti:$storeName:old:$field:"

  def odaKey(storeName: String, field: String, hex16: String, docId: String): String =
    s"ti:$storeName:oda:$field:$hex16:$docId"
  def odaPrefix(storeName: String, field: String): String =
    s"ti:$storeName:oda:$field:"

  def oddKey(storeName: String, field: String, hex16Desc: String, docId: String): String =
    s"ti:$storeName:odd:$field:$hex16Desc:$docId"
  def oddPrefix(storeName: String, field: String): String =
    s"ti:$storeName:odd:$field:"

  // Global traversal index prefix for a store:
  def storeIndexPrefix(storeName: String): String = s"ti:$storeName:"

  // Encoding for index values. This is deliberately stable and human-readable.
  def encodeJson(json: Json): String = json match {
    case Null => "null"
    case Bool(b, _) => if b then "1" else "0"
    case NumInt(l, _) => l.toString
    case NumDec(bd, _) => bd.toString
    case Str(s, _) => s
    case o: Obj => JsonFormatter.Compact(o)
    case a: Arr => JsonFormatter.Compact(a)
  }

  private def hashPart(s: String): String = {
    val bytes = java.security.MessageDigest.getInstance("SHA-1").digest(
      Option(s).getOrElse("").getBytes(java.nio.charset.StandardCharsets.UTF_8)
    )
    // 12 bytes (24 hex chars) is plenty for this usage, keeps keys short.
    bytes.take(12).map("%02x".format(_)).mkString
  }
}


