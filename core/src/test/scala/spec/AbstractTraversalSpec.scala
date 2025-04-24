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

  private def runFixpoint(startId: Id[Node])
                         (traversalFn: Id[Node] => StatefulGraphTraversalEngine[Node, Set[Id[Node]]]): Task[Set[Id[Node]]] = {
    def loop(current: Id[Node], visited: Set[Id[Node]]): Task[Set[Id[Node]]] = {
      db.edges.transaction { implicit tx: Transaction[SimpleEdge] =>
        // perform the step inside the same transaction
        Task.pure(traversalFn(current))
      }.flatMap { engineInst =>
        engineInst.run().flatMap { raw =>
          val result: Set[Id[Node]] = raw.asInstanceOf[Set[Id[Node]]]
          val newVisited: Set[Id[Node]] = visited ++ result
          val unseen: Set[Id[Node]] = newVisited -- visited

          if (unseen.isEmpty) {
            Task.pure(newVisited)
          } else {
            Task.sequence(unseen.toList.map(loop(_, newVisited)))
              .map(_.foldLeft(newVisited)(_ ++ _))
          }
        }
      }
    }

    loop(startId, Set(startId))
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
      def traversalFn(start: Id[Node])(implicit transaction: Transaction[SimpleEdge]): StatefulGraphTraversalEngine[Node, Set[Id[Node]]] =
        GraphTraversalEngine
          .startFrom(start)
          .withState(Set.empty[Id[Node]])
          .step(
            GraphStep.forward(SimpleEdgeModel),
            (id, _, visited) => Task.pure {
              if (visited.contains(id)) TraversalDecision.Skip(visited)
              else TraversalDecision.Continue(visited + id)
            }
          )

      db.edges.transaction { implicit transaction =>
        runFixpoint(Id("A"))(traversalFn).map { result =>
          result should contain allOf(Id("A"), Id("B"), Id("C"), Id("D"))
        }
      }
    }
    "traverse graph in reverse from D to find parents" in {
      def traversalFn(start: Id[Node])(implicit transaction: Transaction[SimpleEdge]): StatefulGraphTraversalEngine[Node, Set[Id[Node]]] =
        GraphTraversalEngine
          .startFrom(start)
          .withState(Set.empty[Id[Node]])
          .step(
            GraphStep.reverse(SimpleEdgeModel),
            (id, _, visited) => Task.pure {
              if (visited.contains(id)) TraversalDecision.Skip(visited)
              else TraversalDecision.Continue(visited + id)
            }
          )

      db.edges.transaction { implicit transaction =>
        runFixpoint(Id("D"))(traversalFn).map { result =>
          result should contain allOf(Id("D"), Id("B"), Id("C"), Id("A"))
        }
      }
    }
    "traverse with depth limitation" in {
      val maxDepth = 1
      db.edges.transaction { implicit tx: Transaction[SimpleEdge] =>
          GraphTraversalEngine
            .startFrom(Id("A"))
            .withState(Set.empty[Id[Node]])
            .step(
              GraphStep.forward(SimpleEdgeModel),
              (id, depth, visited) => Task.pure {
                if (visited.contains(id) || depth > maxDepth)
                  TraversalDecision.Skip(visited)
                else
                  TraversalDecision.Continue(visited + id)
              }
            )
            .run()
        }.map(_.asInstanceOf[Set[Id[Node]]])
        .map { result =>
          result should contain theSameElementsAs Set(Id("A"), Id("B"), Id("C"))
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
  override implicit val rw = RW.gen
  val name = field.index("name", _.name)
}

case class SimpleEdge(_from: Id[Node], _to: Id[Node], _id: Id[SimpleEdge] = Id())
  extends EdgeDocument[SimpleEdge, Node, Node] with Document[SimpleEdge]

object SimpleEdgeModel extends EdgeModel[SimpleEdge, Node, Node] with JsonConversion[SimpleEdge] {
  override implicit lazy val rw = RW.gen
}