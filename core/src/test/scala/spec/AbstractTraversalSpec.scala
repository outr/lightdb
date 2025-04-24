package spec

import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.graph.{EdgeDocument, EdgeModel}
import lightdb.store.{Store, StoreManager}
import lightdb.traversal._
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{Id, LightDB}
import lightdb.transaction.Transaction
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

abstract class AbstractTraversalSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  protected lazy val specName: String = getClass.getSimpleName
  protected lazy val db: DB = new DB

  // Helper method to create a traversal that collects all reachable nodes
  private def collectAllReachable(startId: Id[Node], step: GraphStep[SimpleEdge, Node, Node], 
                                maxDepth: Option[Int] = None)
                                (implicit tx: Transaction[SimpleEdge]): Task[Set[Id[Node]]] = {
    val engine = GraphTraversalEngine(startId, Set.empty[Id[Node]])

    maxDepth match {
      case Some(depth) =>
        // For depth-limited traversal, use step() and run()
        engine.step(
          step,
          (id: Id[Node], currentDepth: Int, visited: Set[Id[Node]]) => Task.pure {
            if (visited.contains(id) || currentDepth > depth)
              TraversalDecision.Skip(visited)
            else
              TraversalDecision.Continue(visited + id)
          }
        ).run().map(_.asInstanceOf[Set[Id[Node]]])

      case None =>
        // For full traversal, use fixpoint
        engine.fixpoint(
          step,
          (id: Id[Node], _: Int, visited: Set[Id[Node]]) => Task.pure {
            if (visited.contains(id)) TraversalDecision.Skip(visited)
            else TraversalDecision.Continue(visited + id)
          }
        ).map(_.asInstanceOf[Set[Id[Node]]])
    }
  }

  specName should {
    "initialize the database" in {
      db.init.succeed
    }
    "insert graph nodes and edges" in {
      for {
        _ <- db.nodes.transaction { implicit tx =>
          db.nodes.insert(List(
            Node("A", Id("A")),
            Node("B", Id("B")),
            Node("C", Id("C")),
            Node("D", Id("D"))
          ))
        }
        _ <- db.edges.transaction { implicit tx =>
          db.edges.insert(List(
            SimpleEdge(Id("A"), Id("B")),
            SimpleEdge(Id("A"), Id("C")),
            SimpleEdge(Id("B"), Id("D")),
            SimpleEdge(Id("C"), Id("D"))
          ))
        }
      } yield succeed
    }
    "traverse graph from A to collect all reachable nodes" in {
      db.edges.transaction { implicit tx =>
        collectAllReachable(Id("A"), GraphStep.forward(SimpleEdgeModel))
          .map { result =>
            result should contain allOf(Id("A"), Id("B"), Id("C"), Id("D"))
          }
      }
    }
    "traverse graph in reverse from D to find parents" in {
      db.edges.transaction { implicit tx =>
        collectAllReachable(Id("D"), GraphStep.reverse(SimpleEdgeModel))
          .map { result =>
            result should contain allOf(Id("D"), Id("B"), Id("C"), Id("A"))
          }
      }
    }
    "traverse with depth limitation" in {
      val maxDepth = 1
      db.edges.transaction { implicit tx =>
        collectAllReachable(Id("A"), GraphStep.forward(SimpleEdgeModel), Some(maxDepth))
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
    val edges: Store[SimpleEdge, SimpleEdgeModel.type] = store(SimpleEdgeModel)
  }
}

case class Node(name: String, _id: Id[Node] = Id()) extends Document[Node]

object Node extends DocumentModel[Node] with JsonConversion[Node] {
  override implicit val rw: RW[Node] = RW.gen

  val name: I[String] = field.index("name", _.name)
}

case class SimpleEdge(_from: Id[Node], _to: Id[Node], _id: Id[SimpleEdge] = Id())
  extends EdgeDocument[SimpleEdge, Node, Node] with Document[SimpleEdge]

object SimpleEdgeModel extends EdgeModel[SimpleEdge, Node, Node] with JsonConversion[SimpleEdge] {
  override implicit lazy val rw: RW[SimpleEdge] = RW.gen
}
