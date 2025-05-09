package spec

import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.graph.{EdgeDocument, EdgeModel}
import lightdb.store.{Store, StoreManager}
import lightdb.traversal._
import lightdb.upgrade.DatabaseUpgrade
import lightdb.LightDB
import lightdb.id.{EdgeId, Id}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid._

import java.nio.file.Path

abstract class AbstractTraversalSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  protected lazy val specName: String = getClass.getSimpleName
  protected lazy val db: DB = new DB

  specName should {
    "initialize the database" in {
      db.init.succeed
    }
    "insert graph nodes and edges" in {
      for {
        _ <- db.nodes.transaction { implicit tx =>
          tx.insert(List(
            Node("A", Id("A")),
            Node("B", Id("B")),
            Node("C", Id("C")),
            Node("D", Id("D"))
          ))
        }
        _ <- db.edges.transaction { implicit tx =>
          tx.insert(List(
            SimpleEdge(Id("A"), Id("B"), "A to B"),
            SimpleEdge(Id("A"), Id("C"), "A to C"),
            SimpleEdge(Id("B"), Id("D"), "B to D"),
            SimpleEdge(Id("C"), Id("D"), "C to D")
          ))
        }
      } yield succeed
    }
    "traverse graph from A to collect all reachable nodes" in {
      db.edges.transaction { implicit tx =>
        tx.traverse(Id[Node]("A"))
          .bfs(GraphStep.forward[SimpleEdge, SimpleEdge.type, Node, Node](SimpleEdge))
          .collectAllReachable()
          .map { result =>
            result should contain allOf(Id("A"), Id("B"), Id("C"), Id("D"))
          }
      }
    }
    "traverse graph in reverse from D to find parents" in {
      db.edges.transaction { implicit tx =>
        tx.traverse(Id[Node]("D"))
          .bfs(GraphStep.reverse[SimpleEdge, SimpleEdge.type, Node, Node](SimpleEdge))
          .collectAllReachable()
          .map { result =>
            result should contain allOf(Id("D"), Id("B"), Id("C"), Id("A"))
          }
      }
    }
    "traverse with depth limitation" in {
      val maxDepth = 1
      db.edges.transaction { implicit tx =>
        tx.traverse(Set(Id[Node]("A")))
          .bfs(GraphStep.forward[SimpleEdge, SimpleEdge.type, Node, Node](SimpleEdge), maxDepth)
          .collectAllReachable()
          .map { result =>
            result should contain theSameElementsAs Set(Id("A"), Id("B"), Id("C"))
          }
      }
    }
    "dispose the database" in {
      db.dispose.next(dispose()).succeed
    }
  }

  def dispose(): Task[Unit] = Task.unit

  def storeManager: StoreManager

  class DB extends LightDB {
    override type SM = StoreManager
    override val storeManager: StoreManager = spec.storeManager

    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    override def upgrades: List[DatabaseUpgrade] = Nil

    val nodes: Store[Node, Node.type] = store(Node)
    val edges: Store[SimpleEdge, SimpleEdge.type] = store(SimpleEdge)
  }
}

case class Node(name: String, _id: Id[Node] = Id()) extends Document[Node]

object Node extends DocumentModel[Node] with JsonConversion[Node] {
  override implicit val rw: RW[Node] = RW.gen

  val name: I[String] = field.index("name", _.name)
}

case class SimpleEdge(_from: Id[Node], _to: Id[Node], name: String, _id: EdgeId[SimpleEdge, Node, Node])
  extends EdgeDocument[SimpleEdge, Node, Node] with Document[SimpleEdge]

object SimpleEdge extends EdgeModel[SimpleEdge, Node, Node] with JsonConversion[SimpleEdge] {
  override implicit lazy val rw: RW[SimpleEdge] = RW.gen

  val name: I[String] = field.index("name", _.name)

  def apply(_from: Id[Node], _to: Id[Node], name: String): SimpleEdge = SimpleEdge(_from, _to, name, EdgeId(_from, _to))
}
