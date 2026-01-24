package lightdb.traversal.store

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.IndexingState
import lightdb.time.Timestamp

import java.util.NavigableMap
import java.util.TreeMap
import scala.collection.mutable

/**
 * In-memory index cache (correctness + performance bootstrap).
 *
 * This is intentionally simple:
 * - equality postings only: (fieldName, value) -> set(docId)
 * - numeric range postings: (fieldName, numericValue) -> set(docId)
 * - rebuilt lazily on first use if empty
 *
 * Future phases can persist this index or replace it with postings/bitmaps + range encodings.
 */
final class TraversalIndexCache[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  storeName: String,
  model: Model,
  val enabled: Boolean
) {
  private val lock = new AnyRef
  private var built: Boolean = false
  private val ngramSize: Int = 3

  // field -> value -> docIds
  private val eq = mutable.HashMap.empty[String, mutable.HashMap[String, mutable.HashSet[String]]]
  // field -> numericValue -> docIds
  private val numLong = mutable.HashMap.empty[String, NavigableMap[Long, mutable.HashSet[String]]]
  private val numDouble = mutable.HashMap.empty[String, NavigableMap[Double, mutable.HashSet[String]]]
  // field -> ngram -> docIds
  private val ngrams = mutable.HashMap.empty[String, mutable.HashMap[String, mutable.HashSet[String]]]

  def ensureBuilt(buildFromDocs: => Iterator[Doc]): Unit = lock.synchronized {
    if !enabled then return
    if built then return
    eq.clear()
    numLong.clear()
    numDouble.clear()
    ngrams.clear()
    val state = new IndexingState
    buildFromDocs.foreach(addDoc(state, _))
    built = true
  }

  def clear(): Unit = lock.synchronized {
    if !enabled then return
    eq.clear()
    numLong.clear()
    numDouble.clear()
    ngrams.clear()
    built = false
  }

  def onInsert(doc: Doc): Unit = lock.synchronized {
    if !enabled then return
    if !built then return
    addDoc(new IndexingState, doc)
  }

  def onUpsert(oldDoc: Option[Doc], newDoc: Doc): Unit = lock.synchronized {
    if !enabled then return
    if !built then return
    val state = new IndexingState
    oldDoc.foreach(removeDoc(state, _))
    addDoc(state, newDoc)
  }

  def onDelete(doc: Doc): Unit = lock.synchronized {
    if !enabled then return
    if !built then return
    removeDoc(new IndexingState, doc)
  }

  def equalsPostings(fieldName: String, value: Any): Set[String] = lock.synchronized {
    if !enabled then return Set.empty
    val required = TraversalIndex.valuesForIndexValue(value).map {
      case null => "null"
      case s: String => s.toLowerCase
      case v => v.toString
    }
    if required.isEmpty then Set.empty
    else {
      val sets = required.map { v =>
        eq.get(fieldName).flatMap(_.get(v)).map(_.toSet).getOrElse(Set.empty[String])
      }
      sets.reduce(_.intersect(_))
    }
  }

  def startsWithPostings(fieldName: String, prefix: String): Set[String] = lock.synchronized {
    if !enabled then return Set.empty
    val p = prefix.toLowerCase
    eq.get(fieldName).toList.flatMap { byValue =>
      byValue.collect { case (v, ids) if v.startsWith(p) => ids }.flatMap(_.toSet)
    }.toSet
  }

  def containsPostings(fieldName: String, query: String): Set[String] = lock.synchronized {
    if !enabled then return Set.empty
    val q = Option(query).getOrElse("").toLowerCase
    if q.length < ngramSize then return Set.empty
    val grams = gramsOf(q)
    val byGram = ngrams.get(fieldName).getOrElse(mutable.HashMap.empty)
    val sets = grams.map(g => byGram.get(g).map(_.toSet).getOrElse(Set.empty[String]))
    if sets.isEmpty then Set.empty else sets.reduce(_ intersect _)
  }

  def rangeLongPostings(fieldName: String, from: Option[Long], to: Option[Long]): Set[String] = lock.synchronized {
    if !enabled then return Set.empty
    val map = numLong.getOrElse(fieldName, new TreeMap[Long, mutable.HashSet[String]]())
    if map.isEmpty then return Set.empty
    val f = from.getOrElse(map.firstKey())
    val t = to.getOrElse(map.lastKey())
    if f > t then return Set.empty
    map.subMap(f, true, t, true).values().toArray.toList
      .asInstanceOf[List[mutable.HashSet[String]]]
      .flatMap(_.toSet)
      .toSet
  }

  def rangeDoublePostings(fieldName: String, from: Option[Double], to: Option[Double]): Set[String] = lock.synchronized {
    if !enabled then return Set.empty
    val map = numDouble.getOrElse(fieldName, new TreeMap[Double, mutable.HashSet[String]]())
    if map.isEmpty then return Set.empty
    val f = from.getOrElse(map.firstKey())
    val t = to.getOrElse(map.lastKey())
    if f > t then return Set.empty
    map.subMap(f, true, t, true).values().toArray.toList
      .asInstanceOf[List[mutable.HashSet[String]]]
      .flatMap(_.toSet)
      .toSet
  }

  private def addDoc(state: IndexingState, doc: Doc): Unit = {
    model.indexedFields.foreach { f =>
      val values = TraversalIndex.valuesForIndex(f, doc, state)
      values.foreach { v =>
        val byValue = eq.getOrElseUpdate(f.name, mutable.HashMap.empty)
        val ids = byValue.getOrElseUpdate(v, mutable.HashSet.empty)
        ids += doc._id.value

        // N-gram postings for string-ish index values (used for contains prefilter).
        if v != null then {
          val s = v.toString.toLowerCase
          if s.length >= ngramSize then {
            val byGram = ngrams.getOrElseUpdate(f.name, mutable.HashMap.empty)
            gramsOf(s).foreach { gram =>
              val gset = byGram.getOrElseUpdate(gram, mutable.HashSet.empty)
              gset += doc._id.value
            }
          }
        }
      }

      // Also attempt numeric indexing for the field values (including Timestamp).
      val raw = f.asInstanceOf[lightdb.field.Field[Doc, Any]].get(doc, f.asInstanceOf[lightdb.field.Field[Doc, Any]], state)
      TraversalIndex.valuesForIndexValue(raw).foreach {
        case null => // ignore
        case t: Timestamp =>
          val map = numLong.getOrElseUpdate(f.name, new TreeMap[Long, mutable.HashSet[String]]())
          val set = Option(map.get(t.value)).getOrElse {
            val hs = mutable.HashSet.empty[String]
            map.put(t.value, hs)
            hs
          }
          set += doc._id.value
        case n: java.lang.Number =>
          val lmap = numLong.getOrElseUpdate(f.name, new TreeMap[Long, mutable.HashSet[String]]())
          val lv = n.longValue()
          val lset = Option(lmap.get(lv)).getOrElse {
            val hs = mutable.HashSet.empty[String]
            lmap.put(lv, hs)
            hs
          }
          lset += doc._id.value

          val dmap = numDouble.getOrElseUpdate(f.name, new TreeMap[Double, mutable.HashSet[String]]())
          val dv = n.doubleValue()
          val dset = Option(dmap.get(dv)).getOrElse {
            val hs = mutable.HashSet.empty[String]
            dmap.put(dv, hs)
            hs
          }
          dset += doc._id.value
        case _ => // ignore
      }
    }
  }

  private def removeDoc(state: IndexingState, doc: Doc): Unit = {
    model.indexedFields.foreach { f =>
      val values = TraversalIndex.valuesForIndex(f, doc, state)
      values.foreach { v =>
        eq.get(f.name).flatMap(_.get(v)).foreach { ids =>
          ids -= doc._id.value
          if ids.isEmpty then {
            eq.get(f.name).foreach(_.remove(v))
          }
        }

        if v != null then {
          val s = v.toString.toLowerCase
          if s.length >= ngramSize then {
            ngrams.get(f.name).foreach { byGram =>
              gramsOf(s).foreach { gram =>
                byGram.get(gram).foreach { ids =>
                  ids -= doc._id.value
                  if ids.isEmpty then byGram.remove(gram)
                }
              }
            }
          }
        }
      }

      val raw = f.asInstanceOf[lightdb.field.Field[Doc, Any]].get(doc, f.asInstanceOf[lightdb.field.Field[Doc, Any]], state)
      TraversalIndex.valuesForIndexValue(raw).foreach {
        case null => // ignore
        case t: Timestamp =>
          numLong.get(f.name).foreach { m =>
            Option(m.get(t.value)).foreach { ids =>
              ids -= doc._id.value
              if ids.isEmpty then m.remove(t.value)
            }
          }
        case n: java.lang.Number =>
          val lv = n.longValue()
          numLong.get(f.name).foreach { m =>
            Option(m.get(lv)).foreach { ids =>
              ids -= doc._id.value
              if ids.isEmpty then m.remove(lv)
            }
          }
          val dv = n.doubleValue()
          numDouble.get(f.name).foreach { m =>
            Option(m.get(dv)).foreach { ids =>
              ids -= doc._id.value
              if ids.isEmpty then m.remove(dv)
            }
          }
        case _ => // ignore
      }
    }
  }

  private def gramsOf(s: String): Set[String] = {
    val n = ngramSize
    if s.length < n then Set.empty
    else (0 to (s.length - n)).iterator.map(i => s.substring(i, i + n)).toSet
  }
}


