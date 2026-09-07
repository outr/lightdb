package spec

import fabric.rw.*
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.sql.SQLState
import lightdb.sql.connect.DataSourceConnectionManager
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import rapid.Task
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.sql.{Connection, SQLException}
import javax.sql.DataSource

@EmbeddedTest
class SQLTransactionCleanupSpec extends AnyWordSpec with Matchers {
  case class Row(_id: Id[Row]) extends Document[Row]
  object Row extends DocumentModel[Row] with JsonConversion[Row] { implicit val rw: RW[Row] = RW.gen }
  private def proxy[A](cls: Class[A])(call: String => AnyRef): A =
    Proxy.newProxyInstance(cls.getClassLoader, Array(cls), new InvocationHandler {
      def invoke(instance: Any, method: Method, args: Array[AnyRef]): AnyRef = call(method.getName)
    }).asInstanceOf[A]
  class Fixture(commitError: Boolean = false, rollbackError: Boolean = false) {
    var commits = 0; var rollbacks = 0; var closes = 0
    val connection = proxy(classOf[Connection]) {
      case "commit" => commits += 1; if (commitError) throw new SQLException("commit failed"); null
      case "rollback" => rollbacks += 1; if (rollbackError) throw new SQLException("rollback failed"); null
      case "close" => closes += 1; null
      case "isClosed" | "getAutoCommit" => java.lang.Boolean.FALSE
      case name => throw new UnsupportedOperationException(name)
    }
    val manager = new DataSourceConnectionManager {
      protected val dataSource: DataSource = proxy(classOf[DataSource]) {
        case "getConnection" => connection
        case name => throw new UnsupportedOperationException(name)
      }
      protected def doDispose(): Task[Unit] = Task.unit
    }
    // These lifecycle paths do not consult store metadata or execute a query.
    val state = SQLState[Row, Row.type](manager, null, false)
    manager.getConnection(state)
  }
  "SQL transaction cleanup" should {
    "propagate JDBC commit failures instead of logging success" in {
      val f = new Fixture(commitError = true)
      intercept[SQLException](f.state.commit.sync()).getMessage shouldBe "commit failed"
      f.commits shouldBe 1
      f.state.rollback.sync()
      f.state.close.sync()
      f.commits shouldBe 1
      f.closes shouldBe 1
    }
    "never commit at datasource release, including read/raw-JDBC transactions" in {
      val f = new Fixture()
      f.manager.releaseConnection(f.state)
      f.commits shouldBe 0
      f.rollbacks shouldBe 1
      f.closes shouldBe 1
      f.manager.currentConnection(f.state) shouldBe None
      f.manager.releaseConnection(f.state)
      f.closes shouldBe 1
    }
    "close and forget a connection even when rollback itself fails" in {
      val f = new Fixture(rollbackError = true)
      intercept[SQLException](f.manager.releaseConnection(f.state))
      f.closes shouldBe 1
      f.manager.currentConnection(f.state) shouldBe None
    }
    "commit raw JDBC work even when no model writer set the dirty flag" in {
      val f = new Fixture()
      f.state.commit.sync()
      f.commits shouldBe 1
      f.state.close.sync()
      f.commits shouldBe 1
    }
  }
}
