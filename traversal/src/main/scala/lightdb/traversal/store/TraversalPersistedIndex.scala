package lightdb.traversal.store

import fabric.{Null, Str}
import fabric.rw._
import lightdb.KeyValue
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.{Field, IndexingState}
import lightdb.id.Id
import lightdb.transaction.PrefixScanningTransaction
import profig.Profig
import rapid._

import scala.collection.mutable.ListBuffer
import java.util.PriorityQueue

/**
 * Persisted postings index stored in a dedicated KeyValue store (TraversalStore.indexBacking / effectiveIndexBacking).
 *
 * This is correctness-first and intentionally minimal:
 * - equality postings for indexed fields (including multi-valued via TraversalIndex.valuesForIndex)
 * - trigram postings for string-ish index values (to prefilter contains/regex)
 *
 * Note: This is NOT atomic with the primary store on backends like RocksDB today; it is a best-effort
 * durability/cold-start optimization. Query execution must remain correct even if postings are missing/stale.
 */
object TraversalPersistedIndex {
  private val N: Int = 3
  private val ClearChunkSize: Int = 1_000
  private val BuildChunkSize: Int = 256
  private val PrefixMaxLen: Int = Profig("lightdb.traversal.persistedIndex.prefixMaxLen").opt[Int].getOrElse(8)
  private val NumericPrefixMaxLen: Int = Profig("lightdb.traversal.persistedIndex.numericPrefixMaxLen").opt[Int].getOrElse(16)
  private val NumericOrderedPrefixLen: Int =
    (Profig("lightdb.traversal.persistedIndex.numericOrderedPrefixLen").opt[Int].getOrElse(4) max 1) min NumericPrefixMaxLen
  private val OneSidedRangeSeeding: Boolean = Profig("lightdb.traversal.persistedIndex.rangeOneSided").opt[Boolean].getOrElse(false)

  private def maxSeedSize: Int = Profig("lightdb.traversal.persistedIndex.maxSeedSize").opt[Int].getOrElse(100_000)

  private val OrderByFieldPostingsEnabled: Boolean =
    Profig("lightdb.traversal.orderByFieldPostings.enabled").opt[Boolean].getOrElse(false)
  private val MaxKeyBytes: Int = 128

  private def safeId(key: String): Option[Id[KeyValue]] = {
    val bytes = Option(key).getOrElse("").getBytes(java.nio.charset.StandardCharsets.UTF_8)
    if (bytes.length <= MaxKeyBytes) Some(Id[KeyValue](key)) else None
  }

  private def invertHex16(hex16: String): String = {
    val u = BigInt(Option(hex16).getOrElse("0"), 16)
    val max = (BigInt(1) << 64) - 1
    f"${(max - u) & max}%016x"
  }

  // Streaming postings API (avoids materialization unless explicitly requested).
  private[traversal] def postingsStream(prefix: String,
                                        kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): rapid.Stream[String] =
    kv.prefixStream(prefix).collect { case KeyValue(_, Str(s, _)) => s }

  private def intersectSortedLists(a: List[String], b: List[String]): List[String] = {
    val ab = a.toArray
    val bb = b.toArray
    var i = 0
    var j = 0
    val out = ListBuffer.empty[String]
    while (i < ab.length && j < bb.length) {
      val cmp = ab(i).compareTo(bb(j))
      if (cmp == 0) {
        out += ab(i)
        i += 1
        j += 1
      } else if (cmp < 0) {
        i += 1
      } else {
        j += 1
      }
    }
    out.result()
  }

  /**
   * Merge multiple **sorted** id lists into a single sorted, de-duped list, bounded by `takeN`.
   *
   * Intended for page-only IndexOrder cases where we materialize a small number of ids per postings prefix and then
   * need a global IndexOrder merge without doing `flatten.distinct.sorted` (which allocates more and sorts the full concat).
   */
  private[traversal] def mergeSortedDistinctTake(lists: List[List[String]], takeN: Int): List[String] = {
    val n = math.max(0, takeN)
    if (n == 0) Nil
    else {
      final case class Node(value: String, listIdx: Int, pos: Int)
      val pq = new PriorityQueue[Node](16, (a: Node, b: Node) => a.value.compareTo(b.value))

      val arr = lists.toArray
      var i = 0
      while (i < arr.length) {
        val l = arr(i)
        if (l.nonEmpty) pq.add(Node(l.head, i, 0))
        i += 1
      }

      val out = ListBuffer.empty[String]
      var last: String = null
      while (!pq.isEmpty && out.length < n) {
        val node = pq.poll()
        val v = node.value
        if (last == null || v != last) {
          out += v
          last = v
        }
        val nextPos = node.pos + 1
        val l = arr(node.listIdx)
        if (nextPos < l.length) pq.add(Node(l(nextPos), node.listIdx, nextPos))
      }
      out.result()
    }
  }

  /**
   * Intersect docId-ordered postings prefixes using **bounded materialization**.
   *
   * This is for page-only `Sort.IndexOrder` execution where we only need the first `takeN` docIds per prefix.
   * Each prefix is scanned in key order, which yields docIds in IndexOrder (lexicographic) order.
   */
  def intersectOrderedPostingsTake(prefixes: List[String],
                                   kv: PrefixScanningTransaction[KeyValue, KeyValue.type],
                                   takeN: Int): Task[List[String]] = {
    val n = math.max(0, takeN)
    val ps = prefixes.distinct
    if (n == 0 || ps.isEmpty) Task.pure(Nil)
    else ps.map(p => postingsTake(p, kv, n)).tasks.map(lists => intersectSortedDistinctTake(lists, n))
  }

  private[traversal] def postingsCount(prefix: String,
                                       kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Int] =
    postingsStream(prefix, kv).count

  private[traversal] def postingsTake(prefix: String,
                                      kv: PrefixScanningTransaction[KeyValue, KeyValue.type],
                                      n: Int): Task[List[String]] =
    postingsStream(prefix, kv).take(n).toList

  /**
   * Single-pass helper for “how big is this postings list (up to max)?” plus “give me the first n ids”.
   *
   * Returns:
   * - `take`: first `takeN` ids (bounded)
   * - `countUpTo`: count of ids observed, capped at `maxCount + 1` (so `> maxCount` can be detected)
   */
  private[traversal] def postingsTakeAndCountUpTo(prefix: String,
                                                  kv: PrefixScanningTransaction[KeyValue, KeyValue.type],
                                                  takeN: Int,
                                                  maxCount: Int): Task[(List[String], Int)] = {
    val n = math.max(0, takeN)
    val m = math.max(0, maxCount)
    if (n == 0 || m == 0) Task.pure((Nil, 0))
    else {
      val cap = (math.max(n, m) + 1) max 1
      postingsStream(prefix, kv).take(cap).toList.map { ids =>
        (ids.take(n), ids.size)
      }
    }
  }

  /**
   * Intersect multiple **sorted** id lists (AND), bounded by `takeN`.
   *
   * Useful for page-only IndexOrder paths where we materialize small per-prefix lists and then intersect them.
   */
  private[traversal] def intersectSortedDistinctTake(lists: List[List[String]], takeN: Int): List[String] = {
    val n = math.max(0, takeN)
    if (n == 0) Nil
    else {
      val sortedLists = lists.filter(_.nonEmpty).sortBy(_.size)
      if (sortedLists.isEmpty) Nil
      else sortedLists.reduceLeft(intersectSortedLists).take(n)
    }
  }

  def markReady(storeName: String, kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Unit] = {
    val k = TraversalKeys.metaReadyKey(storeName)
    kv.upsert(KeyValue(_id = Id[KeyValue](k), json = Null)).unit
  }

  def isReady(storeName: String, kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Boolean] =
    kv.get(Id[KeyValue](TraversalKeys.metaReadyKey(storeName))).map(_.nonEmpty)

  def indexDoc[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
                                                                   storeName: String,
                                                                   model: Model,
                                                                   doc: Doc,
                                                                   kv: PrefixScanningTransaction[KeyValue, KeyValue.type]
                                                                 ): Task[Unit] = Task.defer {
    val postings = postingsForDoc(storeName, model, doc)
    if (postings.isEmpty) Task.unit else kv.upsert(postings).unit
  }

  private[traversal] def postingsForDoc[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
                                                                                            storeName: String,
                                                                                            model: Model,
                                                                                            doc: Doc
                                                                                          ): List[KeyValue] = {
    val state = new IndexingState
    val docId = doc._id.value

    model.indexedFields.flatMap { f0 =>
      val fieldAny = f0.asInstanceOf[Field[Doc, Any]]
      val raw = fieldAny.get(doc, fieldAny, state)
      val rawValues: List[Any] = TraversalIndex.valuesForIndexValue(raw)

      // Per-doc reference mapping for single-valued Id fields:
      // - ti:<store>:ref:<field>:<docId> -> "<refId>"
      val ref: List[KeyValue] = rawValues match {
        case List(id: lightdb.id.Id[?]) =>
          safeId(TraversalKeys.refKey(storeName, f0.name, docId)).toList.map(k => KeyValue(k, Str(id.value)))
        case _ =>
          Nil
      }

      // Track whether an encoded value originated from a String, so we only build string-only postings
      // (ngrams/prefix/reverse-prefix) for string values. This avoids huge write amplification on numeric/timestamp fields.
      val encodedInfo: Map[String, Boolean] =
        rawValues.foldLeft(Map.empty[String, Boolean]) { (m, v) =>
          val (enc, isStr) = v match {
            case null => ("null", false)
            case s: String => (s.toLowerCase, true)
            case other => (other.toString, false)
          }
          m.updated(enc, m.getOrElse(enc, false) || isStr)
        }

      def tokensOf(s: String): List[String] =
        Option(s).getOrElse("").split("\\s+").toList.map(_.trim).filter(_.nonEmpty)

      val other: List[KeyValue] = encodedInfo.toList.flatMap { case (encoded, isStringValue) =>
        val eq = KeyValue(Id[KeyValue](TraversalKeys.eqKey(storeName, f0.name, encoded, docId)), Str(docId))
        val eqo: List[KeyValue] =
          safeId(TraversalKeys.eqoKey(storeName, f0.name, encoded, docId)).toList.map(id => KeyValue(id, Str(docId)))
        if (!isStringValue) eqo :+ eq
        else {
          val s = encoded // already lowercased
          val grams = if (s.length >= N) gramsOf(s).toList else Nil
          val ng = grams.map(g => KeyValue(Id[KeyValue](TraversalKeys.ngKey(storeName, f0.name, g, docId)), Str(docId)))
          val tok: List[KeyValue] =
            if (!f0.isTokenized) Nil
            else {
              val toks = tokensOf(s).distinct
              val unordered = toks.map(t => KeyValue(Id[KeyValue](TraversalKeys.tokKey(storeName, f0.name, t, docId)), Str(docId)))
              val ordered = toks.flatMap { t =>
                safeId(TraversalKeys.tokoKey(storeName, f0.name, t, docId)).toList.map(id => KeyValue(id, Str(docId)))
              }
              unordered ++ ordered
            }
          val sw = prefixesOf(s).map(p => KeyValue(Id[KeyValue](TraversalKeys.swKey(storeName, f0.name, p, docId)), Str(docId)))
          val ew = prefixesOf(s.reverse).map(p => KeyValue(Id[KeyValue](TraversalKeys.ewKey(storeName, f0.name, p, docId)), Str(docId)))
          val swo: List[KeyValue] =
            safeId(TraversalKeys.swoKey(storeName, f0.name, s.take(PrefixMaxLen), docId)).toList.map(id => KeyValue(id, Str(docId)))
          val ewo: List[KeyValue] =
            safeId(TraversalKeys.ewoKey(storeName, f0.name, s.reverse.take(PrefixMaxLen), docId)).toList.map(id => KeyValue(id, Str(docId)))
          eqo ++ (eq :: (ng ++ tok ++ sw ++ ew ++ swo ++ ewo))
        }
      }

      val rangeLong: List[KeyValue] =
        rawValues.flatMap(toLong).distinct.flatMap { lv =>
          val hex16 = encodeSortableLong(lv)
          val rl = numericPrefixesOf(hex16).map { p =>
            KeyValue(Id[KeyValue](TraversalKeys.rlKey(storeName, f0.name, p, docId)), Str(docId))
          }
          val ordered =
            safeId(TraversalKeys.rloKey(storeName, f0.name, hex16.take(NumericOrderedPrefixLen), docId))
              .toList
              .map(id => KeyValue(id, Str(docId)))
          val sort =
            if (!OrderByFieldPostingsEnabled) Nil
            else {
              val sortAsc =
                safeId(TraversalKeys.olaKey(storeName, f0.name, hex16, docId)).toList.map(id => KeyValue(id, Str(docId)))
              val sortDesc = {
                val h = invertHex16(hex16)
                safeId(TraversalKeys.oldKey(storeName, f0.name, h, docId)).toList.map(id => KeyValue(id, Str(docId)))
              }
              sortAsc ++ sortDesc
            }
          rl ++ ordered ++ sort
        }

      val rangeDouble: List[KeyValue] =
        rawValues.flatMap(toDouble).distinct.flatMap { dv =>
          val hex16 = encodeSortableDouble(dv)
          val rd = numericPrefixesOf(hex16).map { p =>
            KeyValue(Id[KeyValue](TraversalKeys.rdKey(storeName, f0.name, p, docId)), Str(docId))
          }
          val ordered =
            safeId(TraversalKeys.rdoKey(storeName, f0.name, hex16.take(NumericOrderedPrefixLen), docId))
              .toList
              .map(id => KeyValue(id, Str(docId)))
          val sort =
            if (!OrderByFieldPostingsEnabled) Nil
            else {
              val sortAsc =
                safeId(TraversalKeys.odaKey(storeName, f0.name, hex16, docId)).toList.map(id => KeyValue(id, Str(docId)))
              val sortDesc = {
                val h = invertHex16(hex16)
                safeId(TraversalKeys.oddKey(storeName, f0.name, h, docId)).toList.map(id => KeyValue(id, Str(docId)))
              }
              sortAsc ++ sortDesc
            }
          rd ++ ordered ++ sort
        }

      ref ++ other ++ rangeLong ++ rangeDouble
    }
  }

  def refGet(storeName: String,
             fieldName: String,
             docId: String,
             kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Option[String]] = {
    safeId(TraversalKeys.refKey(storeName, fieldName, docId)) match {
      case None => Task.pure(None)
      case Some(id) =>
        kv.get(id).map(_.headOption).map {
          case Some(KeyValue(_, Str(s, _))) => Some(s)
          case _ => None
        }
    }
  }

  def deindexDoc[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
                                                                     storeName: String,
                                                                     model: Model,
                                                                     doc: Doc,
                                                                     kv: PrefixScanningTransaction[KeyValue, KeyValue.type]
                                                                   ): Task[Unit] = Task.defer {
    val ids = idsForDoc(storeName, model, doc)
    if (ids.isEmpty) Task.unit else ids.map(kv.delete).tasks.unit
  }

  private[traversal] def idsForDoc[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
                                                                                       storeName: String,
                                                                                       model: Model,
                                                                                       doc: Doc
                                                                                     ): List[Id[KeyValue]] = {
    val state = new IndexingState
    val docId = doc._id.value

    model.indexedFields.flatMap { f0 =>
      val fieldAny = f0.asInstanceOf[Field[Doc, Any]]
      val raw = fieldAny.get(doc, fieldAny, state)
      val rawValues: List[Any] = TraversalIndex.valuesForIndexValue(raw)

      val ref: List[Id[KeyValue]] = rawValues match {
        case List(id: lightdb.id.Id[?]) =>
          safeId(TraversalKeys.refKey(storeName, f0.name, docId)).toList
        case _ =>
          Nil
      }

      val encodedInfo: Map[String, Boolean] =
        rawValues.foldLeft(Map.empty[String, Boolean]) { (m, v) =>
          val (enc, isStr) = v match {
            case null => ("null", false)
            case s: String => (s.toLowerCase, true)
            case other => (other.toString, false)
          }
          m.updated(enc, m.getOrElse(enc, false) || isStr)
        }

      def tokensOf(s: String): List[String] =
        Option(s).getOrElse("").split("\\s+").toList.map(_.trim).filter(_.nonEmpty)

      val other: List[Id[KeyValue]] = encodedInfo.toList.flatMap { case (encoded, isStringValue) =>
        val eq = Id[KeyValue](TraversalKeys.eqKey(storeName, f0.name, encoded, docId))
        val eqo = safeId(TraversalKeys.eqoKey(storeName, f0.name, encoded, docId)).toList
        if (!isStringValue) (eqo :+ eq)
        else {
          val s = encoded // already lowercased
          val grams = if (s.length >= N) gramsOf(s).toList else Nil
          val tok: List[Id[KeyValue]] =
            if (!f0.isTokenized) Nil
            else {
              val toks = tokensOf(s).distinct
              val unordered = toks.map(t => Id[KeyValue](TraversalKeys.tokKey(storeName, f0.name, t, docId)))
              val ordered = toks.flatMap(t => safeId(TraversalKeys.tokoKey(storeName, f0.name, t, docId)).toList)
              unordered ++ ordered
            }
          val sw = prefixesOf(s).map(p => Id[KeyValue](TraversalKeys.swKey(storeName, f0.name, p, docId)))
          val ew = prefixesOf(s.reverse).map(p => Id[KeyValue](TraversalKeys.ewKey(storeName, f0.name, p, docId)))
          val swo = safeId(TraversalKeys.swoKey(storeName, f0.name, s.take(PrefixMaxLen), docId)).toList
          val ewo = safeId(TraversalKeys.ewoKey(storeName, f0.name, s.reverse.take(PrefixMaxLen), docId)).toList
          (eqo :+ eq) ++
            (grams.map(g => Id[KeyValue](TraversalKeys.ngKey(storeName, f0.name, g, docId))) ++ tok ++ sw ++ ew ++ swo ++ ewo)
        }
      }

      val rangeLong: List[Id[KeyValue]] =
        rawValues.flatMap(toLong).distinct.flatMap { lv =>
          val hex16 = encodeSortableLong(lv)
          val rl = numericPrefixesOf(hex16).map(p => Id[KeyValue](TraversalKeys.rlKey(storeName, f0.name, p, docId)))
          val ordered = safeId(TraversalKeys.rloKey(storeName, f0.name, hex16.take(NumericOrderedPrefixLen), docId)).toList
          val sort =
            if (!OrderByFieldPostingsEnabled) Nil
            else {
              val sortAsc = safeId(TraversalKeys.olaKey(storeName, f0.name, hex16, docId)).toList
              val sortDesc = safeId(TraversalKeys.oldKey(storeName, f0.name, invertHex16(hex16), docId)).toList
              sortAsc ++ sortDesc
            }
          rl ++ ordered ++ sort
        }

      val rangeDouble: List[Id[KeyValue]] =
        rawValues.flatMap(toDouble).distinct.flatMap { dv =>
          val hex16 = encodeSortableDouble(dv)
          val rd = numericPrefixesOf(hex16).map(p => Id[KeyValue](TraversalKeys.rdKey(storeName, f0.name, p, docId)))
          val ordered = safeId(TraversalKeys.rdoKey(storeName, f0.name, hex16.take(NumericOrderedPrefixLen), docId)).toList
          val sort =
            if (!OrderByFieldPostingsEnabled) Nil
            else {
              val sortAsc = safeId(TraversalKeys.odaKey(storeName, f0.name, hex16, docId)).toList
              val sortDesc = safeId(TraversalKeys.oddKey(storeName, f0.name, invertHex16(hex16), docId)).toList
              sortAsc ++ sortDesc
            }
          rd ++ ordered ++ sort
        }

      ref ++ other ++ rangeLong ++ rangeDouble
    }
  }

  def clearStore(storeName: String, kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Unit] = Task.defer {
    val prefix = TraversalKeys.storeIndexPrefix(storeName)
    kv
      .prefixStream(prefix)
      .map(_._id)
      .chunk(ClearChunkSize)
      .evalMap { idsChunk =>
        idsChunk.toList.map(kv.delete).tasks.unit
      }
      .drain
  }

  def eqPostings(storeName: String,
                 fieldName: String,
                 value: Any,
                 kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Set[String]] = {
    val encoded = normalizeValue(value)
    if (encoded.isEmpty) Task.pure(Set.empty)
    else encoded.toList.map { v =>
      postingsByPrefix(TraversalKeys.eqPrefix(storeName, fieldName, v), kv)
    }.tasks.map { sets =>
      sets.reduce(_ intersect _)
    }
  }

  def ngPostings(storeName: String,
                 fieldName: String,
                 query: String,
                 kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Set[String]] = {
    val q = Option(query).getOrElse("").toLowerCase
    if (q.length < N) Task.pure(Set.empty)
    else {
      val grams = gramsOf(q)
      grams.toList.map { g =>
        postingsByPrefix(TraversalKeys.ngPrefix(storeName, fieldName, g), kv)
      }.tasks.map { sets =>
        if (sets.isEmpty) Set.empty else sets.reduce(_ intersect _)
      }
    }
  }

  private def postingsByPrefix(prefix: String, kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Set[String]] =
    postingsStream(prefix, kv).toList.map(_.toSet)

  /**
   * Bounded prefix scan used for candidate seeding.
   *
   * Returns None if the postings set would exceed maxSeedSize (to avoid materializing very large candidate sets).
   */
  private def postingsByPrefixSeed(prefix: String,
                                   kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Option[Set[String]]] = {
    val max = math.max(0, maxSeedSize)
    if (max == 0) Task.pure(None)
    else {
      postingsStream(prefix, kv)
        .take(max + 1)
        .toList
        .map { list =>
          if (list.size > max) None else Some(list.toSet)
        }
    }
  }

  private def normalizeValue(value: Any): Set[String] =
    TraversalIndex.valuesForIndexValue(value).map {
      case null => "null"
      case s: String => s.toLowerCase
      case v => v.toString
    }.toSet

  private def gramsOf(s: String): Set[String] = {
    if (s.length < N) Set.empty
    else (0 to (s.length - N)).iterator.map(i => s.substring(i, i + N)).toSet
  }

  def swPostings(storeName: String,
                 fieldName: String,
                 query: String,
                 kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Set[String]] = {
    val q0 = Option(query).getOrElse("").toLowerCase
    if (q0.isEmpty) Task.pure(Set.empty)
    else {
      val q = q0.take(PrefixMaxLen)
      postingsByPrefix(TraversalKeys.swPrefix(storeName, fieldName, q), kv)
    }
  }

  private def prefixesOf(s: String): List[String] = {
    val n = math.min(PrefixMaxLen, s.length)
    if (n <= 0) Nil
    else (1 to n).iterator.map(i => s.substring(0, i)).toList
  }

  private def numericPrefixesOf(hex16: String): List[String] = {
    val h = Option(hex16).getOrElse("")
    val n = math.min(NumericPrefixMaxLen, h.length)
    if (n <= 0) Nil
    else (1 to n).iterator.map(i => h.substring(0, i)).toList
  }

  private[traversal] def encodeSortableLong(v: Long): String = {
    val u = v ^ Long.MinValue
    f"$u%016x"
  }

  private[traversal] def encodeSortableDouble(d: Double): String = {
    val bits = java.lang.Double.doubleToRawLongBits(d)
    val sortable = if ((bits & (1L << 63)) != 0L) ~bits else (bits ^ (1L << 63))
    f"$sortable%016x"
  }

  private def toLong(value: Any): Option[Long] = value match {
    case null => None
    case Some(v) => toLong(v)
    case None => None
    case t: lightdb.time.Timestamp => Some(t.value)
    case n: java.lang.Number => Some(n.longValue())
    case s: String => s.toLongOption
    case _ => None
  }

  private def toDouble(value: Any): Option[Double] = value match {
    case null => None
    case Some(v) => toDouble(v)
    case None => None
    case n: java.lang.Number => Some(n.doubleValue())
    case s: String => s.toDoubleOption
    case _ => None
  }

  private[traversal] def prefixesCoveringRange(fromHex: String, toHex: String): List[String] = {
    val from = BigInt(fromHex, 16)
    val to = BigInt(toHex, 16)
    if (from > to) return Nil
    val max = (BigInt(1) << 64) - 1

    def hex16(u: BigInt): String = {
      val s = u.toString(16)
      ("0" * (16 - s.length)) + s
    }

    def trailingZeroNibbles(u: BigInt): Int = {
      var tz = 0
      while (tz < 16 && ((u >> (tz * 4)) & 0xf) == 0) tz += 1
      tz
    }

    var cur = from
    val out = scala.collection.mutable.ListBuffer.empty[String]
    while (cur <= to && cur <= max) {
      val remaining = to - cur + 1
      val kAlign = trailingZeroNibbles(cur)
      val kRem = ((remaining.bitLength - 1) / 4) max 0
      var k = math.min(kAlign, kRem)
      var block = BigInt(1) << (k * 4)
      while (block > remaining && k > 0) {
        k -= 1
        block = BigInt(1) << (k * 4)
      }
      val prefixLen = 16 - k
      out += hex16(cur).take(prefixLen)
      cur += block
    }
    out.toList
  }

  /**
   * Prefix decomposition for persisted numeric range postings (for streaming seeds).
   *
   * Returns None if one-sided range seeding is disabled and the range is one-sided.
   */
  private[traversal] def rangeLongPrefixesFor(from: Option[Long], to: Option[Long]): Option[List[String]] =
    (from, to) match {
      case (Some(f), Some(t)) =>
        val fromHex = encodeSortableLong(f)
        val toHex = encodeSortableLong(t)
        Some(prefixesCoveringRange(fromHex, toHex).map(_.take(NumericPrefixMaxLen)).distinct)
      case (Some(f), None) if OneSidedRangeSeeding =>
        rangeLongPrefixesFor(Some(f), Some(Long.MaxValue))
      case (None, Some(t)) if OneSidedRangeSeeding =>
        rangeLongPrefixesFor(Some(Long.MinValue), Some(t))
      case _ =>
        None
    }

  private[traversal] def rangeDoublePrefixesFor(from: Option[Double], to: Option[Double]): Option[List[String]] =
    (from, to) match {
      case (Some(f), Some(t)) =>
        val fromHex = encodeSortableDouble(f)
        val toHex = encodeSortableDouble(t)
        Some(prefixesCoveringRange(fromHex, toHex).map(_.take(NumericPrefixMaxLen)).distinct)
      case (Some(f), None) if OneSidedRangeSeeding =>
        rangeDoublePrefixesFor(Some(f), Some(Double.PositiveInfinity))
      case (None, Some(t)) if OneSidedRangeSeeding =>
        rangeDoublePrefixesFor(Some(Double.NegativeInfinity), Some(t))
      case _ =>
        None
    }

  private def hex16(u: BigInt): String = {
    val s = u.toString(16)
    ("0" * (16 - s.length)) + s
  }

  /**
   * Ordered-prefix decomposition for IndexOrder range streaming.
   *
   * We use a fixed prefix length (NumericOrderedPrefixLen) so we only store ONE ordered posting per doc/value.
   * The tradeoff is query fanout (number of prefixes) which is bounded by caller guardrails.
   */
  private[traversal] def rangeLongOrderedPrefixesFor(from: Option[Long], to: Option[Long]): Option[List[String]] =
    (from, to) match {
      case (Some(f), Some(t)) =>
        val fromU = BigInt(encodeSortableLong(f), 16)
        val toU = BigInt(encodeSortableLong(t), 16)
        if (fromU > toU) Some(Nil)
        else {
          val shift = (16 - NumericOrderedPrefixLen) * 4
          val fromBlock = fromU >> shift
          val toBlock = toU >> shift
          val count = (toBlock - fromBlock + 1)
          if (count <= 0) Some(Nil)
          else if (count > Int.MaxValue) None
          else {
            val out = scala.collection.mutable.ListBuffer.empty[String]
            var cur = fromBlock
            while (cur <= toBlock) {
              val start = cur << shift
              out += hex16(start).take(NumericOrderedPrefixLen)
              cur += 1
            }
            Some(out.toList)
          }
        }
      case (Some(f), None) if OneSidedRangeSeeding =>
        rangeLongOrderedPrefixesFor(Some(f), Some(Long.MaxValue))
      case (None, Some(t)) if OneSidedRangeSeeding =>
        rangeLongOrderedPrefixesFor(Some(Long.MinValue), Some(t))
      case _ =>
        None
    }

  private[traversal] def rangeDoubleOrderedPrefixesFor(from: Option[Double], to: Option[Double]): Option[List[String]] =
    (from, to) match {
      case (Some(f), Some(t)) =>
        val fromU = BigInt(encodeSortableDouble(f), 16)
        val toU = BigInt(encodeSortableDouble(t), 16)
        if (fromU > toU) Some(Nil)
        else {
          val shift = (16 - NumericOrderedPrefixLen) * 4
          val fromBlock = fromU >> shift
          val toBlock = toU >> shift
          val count = (toBlock - fromBlock + 1)
          if (count <= 0) Some(Nil)
          else if (count > Int.MaxValue) None
          else {
            val out = scala.collection.mutable.ListBuffer.empty[String]
            var cur = fromBlock
            while (cur <= toBlock) {
              val start = cur << shift
              out += hex16(start).take(NumericOrderedPrefixLen)
              cur += 1
            }
            Some(out.toList)
          }
        }
      case (Some(f), None) if OneSidedRangeSeeding =>
        rangeDoubleOrderedPrefixesFor(Some(f), Some(Double.PositiveInfinity))
      case (None, Some(t)) if OneSidedRangeSeeding =>
        rangeDoubleOrderedPrefixesFor(Some(Double.NegativeInfinity), Some(t))
      case _ =>
        None
    }

  def ewPostings(storeName: String,
                 fieldName: String,
                 query: String,
                 kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Set[String]] = {
    val q0 = Option(query).getOrElse("").toLowerCase
    if (q0.isEmpty) Task.pure(Set.empty)
    else {
      val rev = q0.reverse.take(PrefixMaxLen)
      postingsByPrefix(TraversalKeys.ewPrefix(storeName, fieldName, rev), kv)
    }
  }

  def rangeLongPostings(storeName: String,
                        fieldName: String,
                        from: Option[Long],
                        to: Option[Long],
                        kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Set[String]] =
    (from, to) match {
      case (Some(f), Some(t)) =>
        val fromHex = encodeSortableLong(f)
        val toHex = encodeSortableLong(t)
        val prefixes = prefixesCoveringRange(fromHex, toHex).map(_.take(NumericPrefixMaxLen)).distinct
        if (prefixes.isEmpty) Task.pure(Set.empty)
        else prefixes.toList.map(p => postingsByPrefix(TraversalKeys.rlPrefix(storeName, fieldName, p), kv)).tasks
          .map(_.foldLeft(Set.empty[String])(_ union _))
      case (Some(f), None) if OneSidedRangeSeeding =>
        val fromHex = encodeSortableLong(f)
        val toHex = encodeSortableLong(Long.MaxValue)
        val prefixes = prefixesCoveringRange(fromHex, toHex).map(_.take(NumericPrefixMaxLen)).distinct
        if (prefixes.isEmpty) Task.pure(Set.empty)
        else prefixes.toList.map(p => postingsByPrefix(TraversalKeys.rlPrefix(storeName, fieldName, p), kv)).tasks
          .map(_.foldLeft(Set.empty[String])(_ union _))
      case (None, Some(t)) if OneSidedRangeSeeding =>
        val fromHex = encodeSortableLong(Long.MinValue)
        val toHex = encodeSortableLong(t)
        val prefixes = prefixesCoveringRange(fromHex, toHex).map(_.take(NumericPrefixMaxLen)).distinct
        if (prefixes.isEmpty) Task.pure(Set.empty)
        else prefixes.toList.map(p => postingsByPrefix(TraversalKeys.rlPrefix(storeName, fieldName, p), kv)).tasks
          .map(_.foldLeft(Set.empty[String])(_ union _))
      case _ =>
        // Default: disable one-sided range seeding to avoid materializing near-full candidate sets at scale.
        Task.pure(Set.empty)
    }

  def rangeDoublePostings(storeName: String,
                          fieldName: String,
                          from: Option[Double],
                          to: Option[Double],
                          kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Set[String]] =
    (from, to) match {
      case (Some(f), Some(t)) =>
        val fromHex = encodeSortableDouble(f)
        val toHex = encodeSortableDouble(t)
        val prefixes = prefixesCoveringRange(fromHex, toHex).map(_.take(NumericPrefixMaxLen)).distinct
        if (prefixes.isEmpty) Task.pure(Set.empty)
        else prefixes.toList.map(p => postingsByPrefix(TraversalKeys.rdPrefix(storeName, fieldName, p), kv)).tasks
          .map(_.foldLeft(Set.empty[String])(_ union _))
      case (Some(f), None) if OneSidedRangeSeeding =>
        val fromHex = encodeSortableDouble(f)
        val toHex = encodeSortableDouble(Double.PositiveInfinity)
        val prefixes = prefixesCoveringRange(fromHex, toHex).map(_.take(NumericPrefixMaxLen)).distinct
        if (prefixes.isEmpty) Task.pure(Set.empty)
        else prefixes.toList.map(p => postingsByPrefix(TraversalKeys.rdPrefix(storeName, fieldName, p), kv)).tasks
          .map(_.foldLeft(Set.empty[String])(_ union _))
      case (None, Some(t)) if OneSidedRangeSeeding =>
        val fromHex = encodeSortableDouble(Double.NegativeInfinity)
        val toHex = encodeSortableDouble(t)
        val prefixes = prefixesCoveringRange(fromHex, toHex).map(_.take(NumericPrefixMaxLen)).distinct
        if (prefixes.isEmpty) Task.pure(Set.empty)
        else prefixes.toList.map(p => postingsByPrefix(TraversalKeys.rdPrefix(storeName, fieldName, p), kv)).tasks
          .map(_.foldLeft(Set.empty[String])(_ union _))
      case _ =>
        // Default: disable one-sided range seeding to avoid materializing near-full candidate sets at scale.
        Task.pure(Set.empty)
    }

  /**
   * Seed-friendly postings helpers (bounded by maxSeedSize).
   */
  def eqPostingsCountIfSingleValue(storeName: String,
                                   fieldName: String,
                                   value: Any,
                                   kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Option[Int]] = {
    // Only safe/meaningful for single scalar values; multi-valued equality requires intersection across values.
    // Additionally: string values are normalized to lowercase for indexing, but Equals semantics are case-sensitive,
    // so a postings-count would be an overcount when case differs. Restrict to non-string values.
    val rawValues = TraversalIndex.valuesForIndexValue(value)
    if (rawValues.exists(_.isInstanceOf[String])) Task.pure(None)
    else {
      val encoded = normalizeValue(value)
      if (encoded.size != 1) Task.pure(None)
      else {
        val prefix = TraversalKeys.eqPrefix(storeName, fieldName, encoded.head)
        postingsCount(prefix, kv).map(Some(_))
      }
    }
  }

  /**
   * Exact postings-count for tokenized equals when the query is a single token.
   *
   * Tokenized semantics in traversal/SQL are "all tokens present" (AND). For a single token, total count is just the
   * postings count for that token. This is case-insensitive (tokens are normalized to lowercase), which matches traversal's
   * tokenized evalFilter semantics.
   */
  def tokPostingsCountIfSingleToken(storeName: String,
                                    fieldName: String,
                                    query: String,
                                    kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Option[Int]] = {
    val tokens =
      Option(query)
        .getOrElse("")
        .toLowerCase
        .split("\\s+")
        .toList
        .map(_.trim)
        .filter(_.nonEmpty)
        .distinct
    tokens match {
      case token :: Nil =>
        val prefix = TraversalKeys.tokPrefix(storeName, fieldName, token)
        postingsCount(prefix, kv).map(Some(_))
      case _ =>
        Task.pure(None)
    }
  }

  /**
   * Exact postings-count for tokenized equals when the query has a small number of tokens and each token postings list is
   * small enough to materialize.
   *
   * This is primarily used as a fast exact-total path when `maxSeedSize` prevents materializing candidate ids, but the
   * token postings are still small. It is bounded and will return None if any token postings are too large.
   */
  def tokPostingsCountIfSmallTokens(storeName: String,
                                    fieldName: String,
                                    query: String,
                                    kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Option[Int]] = {
    val maxTokenPostings: Int =
      Profig("lightdb.traversal.persistedIndex.fastTotal.tokenized.maxTokenPostings").opt[Int].getOrElse(100_000) max 1
    val maxTokens: Int =
      Profig("lightdb.traversal.persistedIndex.fastTotal.tokenized.maxTokens").opt[Int].getOrElse(3) max 1

    val tokens =
      Option(query)
        .getOrElse("")
        .toLowerCase
        .split("\\s+")
        .toList
        .map(_.trim)
        .filter(_.nonEmpty)
        .distinct
        .take(maxTokens + 1)

    if (tokens.isEmpty) Task.pure(None)
    else if (tokens.size > maxTokens) Task.pure(None)
    else if (tokens.size == 1) tokPostingsCountIfSingleToken(storeName, fieldName, tokens.head, kv)
    else {
      val prefixes = tokens.map(t => TraversalKeys.tokPrefix(storeName, fieldName, t))
      // Choose the smallest postings list as the base to minimize memory.
      prefixes
        .map(p => postingsTakeAndCountUpTo(p, kv, 1, maxTokenPostings + 1).map { case (_, c) => p -> c })
        .tasks
        .flatMap { pcs =>
          val (bestPrefix, bestCount) = pcs.minBy(_._2)
          if (bestCount > maxTokenPostings) Task.pure(None)
          else {
            // Materialize the base set fully (bounded).
            postingsTakeAndCountUpTo(bestPrefix, kv, bestCount, maxTokenPostings + 1).flatMap { case (baseIds, _) =>
              val base = baseIds.toSet
              val rest = prefixes.filterNot(_ == bestPrefix)
              // Intersect with each remaining token postings set (also bounded).
              rest
                .foldLeft(Task.pure(Option(base))) { (accT, pfx) =>
                  accT.flatMap {
                    case None => Task.pure(None)
                    case Some(current) if current.isEmpty => Task.pure(Some(Set.empty))
                    case Some(current) =>
                      postingsTakeAndCountUpTo(pfx, kv, maxTokenPostings + 1, maxTokenPostings + 1).flatMap { case (ids, c) =>
                        if (c > maxTokenPostings) Task.pure(None)
                        else Task.pure(Some(current intersect ids.toSet))
                      }
                  }
                }
                .map(_.map(_.size))
            }
          }
        }
    }
  }

  def eqSeedPostings(storeName: String,
                     fieldName: String,
                     value: Any,
                     kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Option[Set[String]]] = {
    val encoded = normalizeValue(value)
    // If we can't normalize, treat as unseedable (fallback to scan+verify).
    if (encoded.isEmpty) Task.pure(None)
    else {
      // For multi-value equality, we intersect across encoded values.
      encoded.toList.map { v =>
        postingsByPrefixSeed(TraversalKeys.eqPrefix(storeName, fieldName, v), kv)
      }.tasks.map { opts =>
        if (opts.exists(_.isEmpty)) None
        else Some(opts.flatten.reduce(_ intersect _))
      }
    }
  }

  def ngSeedPostings(storeName: String,
                     fieldName: String,
                     query: String,
                     kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Option[Set[String]]] = {
    val q = Option(query).getOrElse("").toLowerCase
    // Too short to seed reliably with N-grams; treat as unseedable (fallback to scan+verify).
    if (q.length < N) Task.pure(None)
    else {
      val grams = gramsOf(q).toList
      grams.map(g => postingsByPrefixSeed(TraversalKeys.ngPrefix(storeName, fieldName, g), kv)).tasks.map { opts =>
        if (opts.exists(_.isEmpty)) None
        else Some(if (opts.isEmpty) Set.empty else opts.flatten.reduce(_ intersect _))
      }
    }
  }

  def tokSeedPostings(storeName: String,
                      fieldName: String,
                      query: String,
                      kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Option[Set[String]]] = {
    val tokens = Option(query).getOrElse("").toLowerCase.split("\\s+").toList.map(_.trim).filter(_.nonEmpty).distinct
    // Empty token query is treated as a no-op predicate in SQL (1=1). For traversal candidate seeding, we return
    // an empty set to signal "can't seed meaningfully" but still allow tokenized evalFilter to behave correctly.
    if (tokens.isEmpty) Task.pure(None)
    else {
      tokens.map(t => postingsByPrefixSeed(TraversalKeys.tokPrefix(storeName, fieldName, t), kv)).tasks.map { opts =>
        if (opts.exists(_.isEmpty)) None
        else Some(if (opts.isEmpty) Set.empty else opts.flatten.reduce(_ intersect _))
      }
    }
  }

  def swSeedPostings(storeName: String,
                     fieldName: String,
                     query: String,
                     kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Option[Set[String]]] = {
    val q0 = Option(query).getOrElse("").toLowerCase
    // Empty prefix matches everything; avoid seeding massive candidate sets.
    if (q0.isEmpty) Task.pure(None)
    else postingsByPrefixSeed(TraversalKeys.swPrefix(storeName, fieldName, q0.take(PrefixMaxLen)), kv)
  }

  def ewSeedPostings(storeName: String,
                     fieldName: String,
                     query: String,
                     kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Option[Set[String]]] = {
    val q0 = Option(query).getOrElse("").toLowerCase
    // Empty suffix matches everything; avoid seeding massive candidate sets.
    if (q0.isEmpty) Task.pure(None)
    else postingsByPrefixSeed(TraversalKeys.ewPrefix(storeName, fieldName, q0.reverse.take(PrefixMaxLen)), kv)
  }

  def rangeLongSeedPostings(storeName: String,
                            fieldName: String,
                            from: Option[Long],
                            to: Option[Long],
                            kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Option[Set[String]]] =
    (from, to) match {
      case (Some(f), Some(t)) =>
        val fromHex = encodeSortableLong(f)
        val toHex = encodeSortableLong(t)
        val prefixes = prefixesCoveringRange(fromHex, toHex).map(_.take(NumericPrefixMaxLen)).distinct
        prefixes.toList.map(p => postingsByPrefixSeed(TraversalKeys.rlPrefix(storeName, fieldName, p), kv)).tasks.map { opts =>
          if (opts.exists(_.isEmpty)) None
          else Some(opts.flatten.foldLeft(Set.empty[String])(_ union _))
        }
      case (Some(f), None) if OneSidedRangeSeeding =>
        rangeLongSeedPostings(storeName, fieldName, Some(f), Some(Long.MaxValue), kv)
      case (None, Some(t)) if OneSidedRangeSeeding =>
        rangeLongSeedPostings(storeName, fieldName, Some(Long.MinValue), Some(t), kv)
      case _ =>
        Task.pure(None)
    }

  def rangeDoubleSeedPostings(storeName: String,
                              fieldName: String,
                              from: Option[Double],
                              to: Option[Double],
                              kv: PrefixScanningTransaction[KeyValue, KeyValue.type]): Task[Option[Set[String]]] =
    (from, to) match {
      case (Some(f), Some(t)) =>
        val fromHex = encodeSortableDouble(f)
        val toHex = encodeSortableDouble(t)
        val prefixes = prefixesCoveringRange(fromHex, toHex).map(_.take(NumericPrefixMaxLen)).distinct
        prefixes.toList.map(p => postingsByPrefixSeed(TraversalKeys.rdPrefix(storeName, fieldName, p), kv)).tasks.map { opts =>
          if (opts.exists(_.isEmpty)) None
          else Some(opts.flatten.foldLeft(Set.empty[String])(_ union _))
        }
      case (Some(f), None) if OneSidedRangeSeeding =>
        rangeDoubleSeedPostings(storeName, fieldName, Some(f), Some(Double.PositiveInfinity), kv)
      case (None, Some(t)) if OneSidedRangeSeeding =>
        rangeDoubleSeedPostings(storeName, fieldName, Some(Double.NegativeInfinity), Some(t), kv)
      case _ =>
        Task.pure(None)
    }

  /**
   * Backfill/rebuild the persisted index from the primary store.
   *
   * This clears any existing postings, indexes all current documents, and then marks the index as ready.
   *
   * NOTE: This can be expensive on large datasets; intended for an explicit maintenance step.
   */
  def buildFromStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
                                                                         storeName: String,
                                                                         model: Model,
                                                                         backing: PrefixScanningTransaction[Doc, Model],
                                                                         kv: PrefixScanningTransaction[KeyValue, KeyValue.type]
                                                                       ): Task[Unit] =
    clearStore(storeName, kv).next {
      backing
        .stream
        .chunk(BuildChunkSize)
        .evalMap { docsChunk =>
          docsChunk.toList.map { d =>
            indexDoc(storeName, model, d, kv)
          }.tasks.unit
        }
        .drain
        .next(markReady(storeName, kv))
    }
}


