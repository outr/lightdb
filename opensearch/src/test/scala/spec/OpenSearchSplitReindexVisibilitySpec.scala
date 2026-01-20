package spec

import fabric.rw.*
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.progress.ProgressManager
import lightdb.store.CollectionManager
import lightdb.store.hashmap.HashMapStore
import lightdb.store.split.SplitStoreManager
import lightdb.time.Timestamp
import lightdb.{LightDB, Sort}
import lightdb.opensearch.OpenSearchStore
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.{AsyncTaskSpec, Task}

import scala.concurrent.duration.DurationInt

/**
 * Reproduction harness for a correctness issue observed in LogicalNetwork:
 *
 * - A SplitCollection has storage updated while search updates are disabled (search is intentionally stale).
 * - A full reIndex() is executed to resync searching from storage.
 * - Immediately after reIndex completes, a searching query occasionally returns 0 / stale results unless a delay is added.
 *
 * This spec is meant to fail if LightDB returns from reIndex before the OpenSearch index is actually searchable.
 *
 * NOTE: This is intentionally a "stress" spec (looped) to increase the chance of catching a race.
 */
@EmbeddedTest
class OpenSearchSplitReindexVisibilitySpec
  extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers
    with OpenSearchTestSupport {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // IMPORTANT: OpenSearchTestSupport defaults refreshPolicy=true for determinism.
    // This bug is observed when relying on "refresh-on-commit", so we remove the policy override for this spec.
    // (OpenSearchTransaction will still refresh on commit.)
    Profig("lightdb.opensearch.refreshPolicy").remove()
  }

  private lazy val db: DB = new DB

  "OpenSearch SplitCollection reIndex visibility" should {
    "make docs searchable immediately after reIndex returns (no sleep required)" in {
      // Keep this reasonably small; the bug should be deterministic, and we don't want this spec to be slow.
      val iterations = 10

      def oneIteration(i: Int): Task[Unit] = {
        val id1 = Row.id(s"r:$i:1")
        val id2 = Row.id(s"r:$i:2")

        // Phase 1: write to storage with search updates disabled
        val writeT = db.rows.transaction { tx =>
          tx.disableSearchUpdate()
          tx.upsert(Row(group = s"g-$i", value = 1, _id = id1))
            .next(tx.upsert(Row(group = s"g-$i", value = 2, _id = id2)))
            .unit
        }

        // Phase 2: ensure searching is out-of-sync (optional sanity check)
        val staleCheckT = db.rows.searching.transaction { stx =>
          stx.query.filter(_.group === s"g-$i").count.map { c =>
            // Should usually be 0 (search updates disabled), but don't assert hard (other tests may run concurrently).
            if (c != 0) {
              scribe.info(s"[OpenSearchSplitReindexVisibilitySpec] pre-reIndex searching already has docs for iteration=$i count=$c")
            }
          }
        }

        // Phase 3: reIndex (truncate searching + stream from storage) and then immediately query searching
        val reindexT = db.rows.reIndex(ProgressManager.none).unit

        val immediateQueryT = db.rows.searching.transaction { stx =>
          // Sort by group then value to mirror "sorted grouping" assumptions used by LN rebuild logic.
          stx.query
            .filter(_.group === s"g-$i")
            .sort(Sort.ByField(Row.group), Sort.ByField(Row.value))
            .clearLimit
            .stream
            .toList
            .map { list =>
              if (list.isEmpty) {
                throw new RuntimeException(s"Immediate searching query returned 0 docs after reIndex (iteration=$i)")
              }
              list.map(_.value) should be(List(1, 2))
              ()
            }
        }

        // If the immediate query fails due to visibility lag, the user reports that a small delay fixes it.
        // We capture that as extra diagnostics to prove "delay makes it pass" if the failure is intermittent.
        val delayedQueryT: Task[Unit] = db.rows.searching.transaction { stx =>
          Task.sleep(2.seconds).next {
            stx.query
              .filter(_.group === s"g-$i")
              .count
              .map { c =>
                scribe.info(s"[OpenSearchSplitReindexVisibilitySpec] delayed searching count iteration=$i count=$c")
                ()
              }
          }.unit
        }

        writeT
          .next(staleCheckT)
          .next(reindexT)
          .next(immediateQueryT)
          .handleError { t =>
            // Add useful context if we hit the suspected lag.
            val ts = Timestamp().value
            scribe.error(s"[OpenSearchSplitReindexVisibilitySpec] FAILURE iteration=$i ts=$ts: ${t.getMessage}", t)
            delayedQueryT.next(Task.error(t))
          }
      }

      db.init.next {
        (0 until iterations).foldLeft(Task.unit) { (acc, i) =>
          acc.next(oneIteration(i))
        }
      }.guarantee(db.truncate().unit).succeed
    }
  }

  class DB extends LightDB {
    override type SM = SplitStoreManager[HashMapStore.type, OpenSearchStore.type]
    override val storeManager: SplitStoreManager[HashMapStore.type, OpenSearchStore.type] =
      SplitStoreManager(HashMapStore, OpenSearchStore)
    override def name: String = "OpenSearchSplitReindexVisibilitySpec"
    override def directory = None
    override def upgrades = Nil

    val rows = store(Row)
  }

  case class Row(group: String,
                 value: Int,
                 created: Timestamp = Timestamp(),
                 modified: Timestamp = Timestamp(),
                 _id: Id[Row] = Row.id()) extends RecordDocument[Row]

  object Row extends RecordDocumentModel[Row] with JsonConversion[Row] {
    override implicit val rw: RW[Row] = RW.gen
    val group: I[String] = field.index(_.group)
    val value: I[Int] = field.index(_.value)
  }
}

