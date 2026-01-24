package lightdb.traversal.graph

import fabric.{Null, NumInt, Str}
import lightdb.KeyValue
import lightdb.id.Id
import lightdb.transaction.PrefixScanningTransaction
import rapid.Task
import rapid.taskSeq2Ops

import scala.collection.mutable

/**
 * Generic disjoint-set / union-find data structure.
 *
 * This is intentionally domain-agnostic:
 * - nodes are identified by string ids
 * - edges are undirected (callers decide what edges to union)
 * - no scoring, thresholds, or “merge semantics” live here
 */
trait DisjointSet {
  def add(id: String): Task[Unit]
  def find(id: String): Task[String]
  def union(a: String, b: String): Task[Unit]
}

object DisjointSet {
  def inMemory(): DisjointSet = new InMemoryDisjointSet

  /**
   * KeyValue-backed disjoint-set.
   *
   * This is designed for very large graphs where an in-memory parent map is not feasible.
   * It stores parent pointers and ranks in the provided KeyValue transaction.
   */
  def keyValue(kv: PrefixScanningTransaction[KeyValue, KeyValue.type], namespace: String): DisjointSet =
    new KeyValueDisjointSet(kv, namespace)
}

final class InMemoryDisjointSet extends DisjointSet {
  private val parent = mutable.HashMap.empty[String, String]
  private val rank = mutable.HashMap.empty[String, Int]

  private def ensure(id: String): Unit = {
    if !parent.contains(id) then {
      parent.put(id, id)
      rank.put(id, 0)
    }
  }

  private def findSync(id0: String): String = {
    val id = Option(id0).getOrElse("")
    ensure(id)

    // Find root
    var r = parent(id)
    while r != parent(r) do {
      r = parent(r)
    }

    // Path compression
    var cur = id
    while parent(cur) != r do {
      val next = parent(cur)
      parent.put(cur, r)
      cur = next
    }

    r
  }

  override def add(id: String): Task[Unit] = Task(ensure(Option(id).getOrElse("")))

  override def find(id: String): Task[String] = Task(findSync(id))

  override def union(a: String, b: String): Task[Unit] = Task {
    val ra = findSync(a)
    val rb = findSync(b)
    if ra != rb then {
      val rka = rank.getOrElse(ra, 0)
      val rkb = rank.getOrElse(rb, 0)
      if rka < rkb then {
        parent.put(ra, rb)
      } else if rkb < rka then {
        parent.put(rb, ra)
      } else {
        parent.put(rb, ra)
        rank.put(ra, rka + 1)
      }
    }
  }
}

final class KeyValueDisjointSet(
  kv: PrefixScanningTransaction[KeyValue, KeyValue.type],
  namespace: String,
) extends DisjointSet {
  private val ns = Option(namespace).getOrElse("default")

  private def pKey(id: String): String = s"ds:$ns:p:$id"
  private def rKey(id: String): String = s"ds:$ns:r:$id"

  private def getParent(id: String): Task[Option[String]] =
    kv.get(Id[KeyValue](pKey(id))).map(_.map {
      case KeyValue(_, Str(s, _)) => s
      case KeyValue(_, other) => other.toString
    })

  private def setParent(id: String, parentId: String): Task[Unit] =
    kv.upsert(KeyValue(_id = Id[KeyValue](pKey(id)), json = Str(parentId))).unit

  private def getRank(id: String): Task[Int] =
    kv.get(Id[KeyValue](rKey(id))).map {
      case Some(KeyValue(_, NumInt(n, _))) => n.toInt
      case Some(KeyValue(_, Str(s, _))) => s.toIntOption.getOrElse(0)
      case Some(_) => 0
      case None => 0
    }

  private def setRank(id: String, value: Int): Task[Unit] =
    kv.upsert(KeyValue(_id = Id[KeyValue](rKey(id)), json = NumInt(value.toLong))).unit

  override def add(id0: String): Task[Unit] = {
    val id = Option(id0).getOrElse("")
    getParent(id).flatMap {
      case Some(_) => Task.unit
      case None =>
        // Initialize parent pointer + rank.
        setParent(id, id).next(setRank(id, 0))
    }
  }

  override def find(id0: String): Task[String] = {
    val id = Option(id0).getOrElse("")

    // Iterative find with path compression.
    def loop(cur: String, path: List[String]): Task[(String, List[String])] = {
      getParent(cur).flatMap {
        case None =>
          // Not initialized yet.
          add(cur).map(_ => (cur, path))
        case Some(p) if p == cur =>
          Task.pure((cur, path))
        case Some(p) =>
          loop(p, cur :: path)
      }
    }

    for
      tup <- loop(id, Nil)
      (root, path) = tup
      _ <- path.map(n => setParent(n, root)).tasks.unit
    yield root
  }

  override def union(a0: String, b0: String): Task[Unit] = {
    val a = Option(a0).getOrElse("")
    val b = Option(b0).getOrElse("")
    for
      _ <- add(a)
      _ <- add(b)
      ra <- find(a)
      rb <- find(b)
      _ <-
        if ra == rb then Task.unit
        else for
          rka <- getRank(ra)
          rkb <- getRank(rb)
          _ <-
            if rka < rkb then setParent(ra, rb)
            else if rkb < rka then setParent(rb, ra)
            else setParent(rb, ra).next(setRank(ra, rka + 1))
        yield ()
    yield ()
  }
}

