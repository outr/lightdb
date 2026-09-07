package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.graph.{EdgeDocument, EdgeModel, ReverseEdgeDocument}
import lightdb.id.{EdgeId, Id}
import lightdb.sql.SQLiteStore
import lightdb.store.CollectionManager
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import rapid.Task

@EmbeddedTest
class SQLiteReverseEdgeFailureSpec extends AnyWordSpec with Matchers {
  case class Node(_id: Id[Node] = Id[Node]()) extends Document[Node]
  object Node extends DocumentModel[Node] with JsonConversion[Node] { implicit val rw: RW[Node] = RW.gen }
  case class Link(_from: Id[Node], _to: Id[Node], _id: EdgeId[Link, Node, Node]) extends EdgeDocument[Link, Node, Node]
  object Link extends EdgeModel[Link, Node, Node] with JsonConversion[Link] { implicit val rw: RW[Link] = RW.gen }

  class Fixture extends LightDB {
    type SM = CollectionManager
    val storeManager: CollectionManager = SQLiteStore
    val directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil
    val links = store(Link)()
    val reverse = reverseStore(ReverseEdgeDocument.createModel[Link, Node, Node]("reverse"), links)
  }

  "Reverse edge store" should {
    "not keep reverse edges when the forward transaction fails" in {
      val db = new Fixture
      db.init.sync()
      try {
        val a = Id[Node]("a")
        val b = Id[Node]("b")
        intercept[IllegalStateException] {
          db.links.transaction { tx =>
            tx.insert(Link(a, b, EdgeId(a, b))).next(Task.error(new IllegalStateException("body")))
          }.sync()
        }
        db.links.transaction(_.count).sync() shouldBe 0
        db.reverse.transaction.active shouldBe 0
        db.reverse.transaction(_.count).sync() shouldBe 0
      } finally db.dispose.sync()
    }
    "release the reverse transaction when the forward commit fails" in {
      val db = new Fixture
      db.init.sync()
      try {
        val a = Id[Node]("a")
        val b = Id[Node]("b")
        val hook = new lightdb.trigger.StoreTrigger[Link, Link.type] {
          override def transactionEnd(tx: lightdb.transaction.Transaction[Link, Link.type]): Task[Unit] =
            Task.error(new IllegalStateException("end hook"))
        }
        db.links.trigger += hook
        intercept[IllegalStateException] {
          db.links.transaction(_.insert(Link(a, b, EdgeId(a, b)))).sync()
        }
        db.links.trigger -= hook
        db.links.transaction.active shouldBe 0
        db.reverse.transaction.active shouldBe 0
        db.reverse.transaction(_.count).sync() shouldBe 0
      } finally db.dispose.sync()
    }
  }
}
