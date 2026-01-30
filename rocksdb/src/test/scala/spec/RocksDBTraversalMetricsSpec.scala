package spec

import fabric.rw.*
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.traversal.store.TraversalManager
import lightdb.traversal.store.TraversalMetrics
import lightdb.upgrade.DatabaseUpgrade
import lightdb.LightDB
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

@EmbeddedTest
class RocksDBTraversalMetricsSpec
  extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers
    with TraversalRocksDBWrappedManager {
  private lazy val specName: String = getClass.getSimpleName

  object DB extends LightDB {
    override type SM = TraversalManager
    override val storeManager: TraversalManager = traversalStoreManager
    override lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))
    override def upgrades: List[DatabaseUpgrade] = Nil

    val docs: S[Doc, Doc.type] = store(Doc)
  }

  specName should {
    "record scan fallback metrics" in {
      Profig("lightdb.traversal.metrics.enabled").store(true)
      val queryTask =
        for
          _ <- DB.init
          _ <- DB.docs.transaction(_.truncate)
          _ <- DB.docs.transaction(_.insert(List(Doc("alpha", Id("a")), Doc("beta", Id("b")))))
          _ <- DB.docs.transaction { tx =>
            tx.query.filter(_.name.contains("al")).toList
          }
          snap = TraversalMetrics.snapshot()
          _ <- DB.dispose
        yield snap.scanFallbackCount should be > 0L
      queryTask
    }
  }
}

case class Doc(name: String, _id: Id[Doc] = Id()) extends Document[Doc]

object Doc extends DocumentModel[Doc] with JsonConversion[Doc] {
  override implicit val rw: RW[Doc] = RW.gen
  val name: I[String] = field.index(_.name)
}
