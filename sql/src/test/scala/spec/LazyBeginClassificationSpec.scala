package spec

import lightdb.sql.connect.LazyBeginConnection
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** Which statements a lazily begun connection may run before its transaction opens. */
@EmbeddedTest
class LazyBeginClassificationSpec extends AnyWordSpec with Matchers {
  import LazyBeginConnection.{boundedRead, readsOnly}

  "readsOnly" should {
    "accept plain queries" in {
      readsOnly("SELECT * FROM t WHERE a = ?") shouldBe true
      readsOnly("  (SELECT a FROM t) UNION (SELECT a FROM u)") shouldBe true
      readsOnly("WITH x AS (SELECT a FROM t) SELECT * FROM x") shouldBe true
      readsOnly("SHOW TRANSACTION ISOLATION LEVEL") shouldBe true
    }
    "reject writes, DDL and anything that locks or advances a sequence" in {
      List(
        "INSERT INTO t VALUES (?)",
        "UPDATE t SET a = ?",
        "DELETE FROM t",
        "MERGE INTO t USING u ON (t.a = u.a) WHEN MATCHED THEN DELETE",
        "CREATE TABLE t (a INT)",
        "WITH gone AS (DELETE FROM t RETURNING a) SELECT * FROM gone",
        "SELECT * FROM t WHERE a = ? FOR UPDATE",
        "SELECT * FROM t FOR NO KEY UPDATE",
        "SELECT * FROM t FOR SHARE",
        "SELECT nextval('s')",
        "SELECT pg_advisory_xact_lock(1)",
        "SET LOCAL statement_timeout = 0",
        "LOCK TABLE t"
      ).foreach(sql => withClue(sql)(readsOnly(sql) shouldBe false))
    }
  }

  "boundedRead" should {
    val none = Map.empty[Int, Long]
    "treat a statement without a fetch size as bounded" in {
      boundedRead("SELECT * FROM t", 0, 0, none) shouldBe true
    }
    "bound a read by a literal or parameter LIMIT within the fetch size" in {
      boundedRead("SELECT * FROM t LIMIT 10", 1000, 0, none) shouldBe true
      boundedRead("SELECT * FROM t WHERE a = ? AND b = ? LIMIT ? OFFSET ?", 1000, 0, Map(1 -> 5L, 2 -> 5000L, 3 -> 20L, 4 -> 0L)) shouldBe true
      boundedRead("SELECT * FROM t WHERE a = ? LIMIT ?", 1000, 0, Map(1 -> 1L, 2 -> 5000L)) shouldBe false
      boundedRead("SELECT * FROM t LIMIT 5000", 1000, 0, none) shouldBe false
    }
    "count only placeholders outside quotes and driver escapes" in {
      boundedRead("SELECT * FROM t WHERE a = '?' AND b ?? 'k' AND c = ? LIMIT ?", 1000, 0, Map(1 -> 9999L, 2 -> 10L)) shouldBe true
    }
    "ignore a LIMIT inside a subquery" in {
      boundedRead("SELECT * FROM t WHERE a IN (SELECT a FROM u LIMIT 5)", 1000, 0, none) shouldBe false
      boundedRead("SELECT * FROM (SELECT * FROM u LIMIT 5000) x LIMIT 3", 1000, 0, none) shouldBe true
    }
    "bound a count and a statement capped by its maximum rows" in {
      boundedRead("SELECT COUNT(*) FROM (SELECT * FROM t) AS c", 1000, 0, none) shouldBe true
      boundedRead("SELECT * FROM t", 1000, 100, none) shouldBe true
    }
    "treat an unknown parameter as unbounded" in {
      boundedRead("SELECT * FROM t LIMIT ?", 1000, 0, none) shouldBe false
      boundedRead("SELECT * FROM t", 1000, 0, none) shouldBe false
    }
  }
}
