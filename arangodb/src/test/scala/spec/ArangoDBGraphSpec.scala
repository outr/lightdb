package spec

import com.arangodb.ArangoDB
import fabric.rw.*
import lightdb.LightDB
import lightdb.arangodb.{ArangoDBStore, ArangoDBStoreManager}
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.graph.EdgeDocument
import lightdb.id.{EdgeId, Id}
import lightdb.store.prefix.PrefixScanningStoreManager
import lightdb.traversal.syntax.*
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path

/** Verifies ArangoDB's native AQL graph operations: OUTBOUND reachability and SHORTEST_PATH. */
@EmbeddedTest
class ArangoDBGraphSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with ArangoDBAvailability {
  private val dbName = "ArangoDBGraphSpec"

  ArangoDBTestSupport.config.foreach { c =>
    val client = new ArangoDB.Builder().host(c.host, c.port).user(c.user).password(c.password).build()
    try { val db = client.db(dbName); if (db.exists()) db.drop() } finally client.shutdown()
  }

  "ArangoDB native graph" should {
    "initialize" in {
      DB.init.succeed
    }
    "insert nodes and links" in {
      for {
        _ <- DB.nodes.transaction(_.insert(List("A", "B", "C", "D").map(n => Node(n, Id[Node](n)))))
        // A->B->C->D and a direct shortcut A->D
        _ <- DB.links.transaction(_.insert(List(
          Link(Id[Node]("A"), Id[Node]("B")),
          Link(Id[Node]("B"), Id[Node]("C")),
          Link(Id[Node]("C"), Id[Node]("D")),
          Link(Id[Node]("A"), Id[Node]("D"))
        )))
      } yield succeed
    }
    "find all reachable nodes natively (OUTBOUND)" in {
      DB.links.transaction { tx =>
        tx.traverse.bfs[Link, Node, Node](Id[Node]("A")).targetIds.toList.map { ids =>
          ids.toSet should be(Set(Id[Node]("A"), Id[Node]("B"), Id[Node]("C"), Id[Node]("D")))
        }
      }
    }
    "find the shortest path natively (SHORTEST_PATH)" in {
      DB.links.transaction { tx =>
        tx.traverse.shortestPaths[Link, Node, Node](Id[Node]("A"), Id[Node]("D")).toList.map { paths =>
          paths.map(_.length) should be(List(1)) // the direct A->D shortcut, not A->B->C->D
        }
      }
    }
    "dispose" in {
      DB.truncate().flatMap(_ => DB.dispose).succeed
    }
  }

  case class Node(name: String, _id: Id[Node] = Id()) extends Document[Node]
  object Node extends DocumentModel[Node] with JsonConversion[Node] {
    override implicit val rw: RW[Node] = RW.gen
    val name: I[String] = field.index("name", _.name)
  }

  case class Link(_from: Id[Node], _to: Id[Node], _id: EdgeId[Link, Node, Node]) extends EdgeDocument[Link, Node, Node]
  object Link extends DocumentModel[Link] with JsonConversion[Link] {
    override implicit val rw: RW[Link] = RW.gen
    val from: I[Id[Node]] = field.index("_from", _._from)
    val to: I[Id[Node]] = field.index("_to", _._to)
    def apply(from: Id[Node], to: Id[Node]): Link = Link(from, to, EdgeId(from, to))
  }

  object DB extends LightDB {
    override type SM = ArangoDBStoreManager
    override val storeManager: ArangoDBStoreManager = ArangoDBStoreManager(ArangoDBTestSupport.config.get, databaseName = Some(dbName))
    override def name: String = dbName
    override lazy val directory: Option[Path] = None
    val nodes: ArangoDBStore[Node, Node.type] = store(Node)()
    val links: ArangoDBStore[Link, Link.type] = store(Link)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
