package spec

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.postgresql.{PostgreSQLStore, PostgreSQLStoreManager}
import lightdb.sql.connect.DataSourceConnectionManager
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}
import java.nio.file.Path
import java.sql.{Connection, DriverManager, SQLException}
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

/**
 * Lazy begin on PostgreSQL: a transaction that only read sends no COMMIT or ROLLBACK, a transaction that wrote commits
 * (or rolls back) exactly as before, a read that may return more rows than its fetch size still streams inside a
 * transaction, a locking read holds its lock until the commit, and connections at REPEATABLE READ are left alone.
 * The physical connections are counted beneath the lazy wrapper.
 */
@EmbeddedTest
class PostgreSQLLazyBeginSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with BeforeAndAfterAll with PostgreSQLAvailability {
  private val schema = "PostgreSQLLazyBeginSpec"

  case class Item(name: String,
                  n: Int,
                  created: Timestamp = Timestamp(),
                  modified: Timestamp = Timestamp(),
                  _id: Id[Item] = Item.id()) extends RecordDocument[Item]

  object Item extends RecordDocumentModel[Item] with JsonConversion[Item] {
    override implicit val rw: RW[Item] = RW.gen
    val name: I[String] = field.index(_.name)
    val n: I[Int] = field.index(_.n)
  }

  /** What reached the physical connections. */
  class Counts {
    val commits = new AtomicInteger
    val rollbacks = new AtomicInteger
    def snapshot: (Int, Int) = (commits.get, rollbacks.get)
  }

  private def counted(raw: Connection, counts: Counts): Connection =
    Proxy.newProxyInstance(getClass.getClassLoader, Array[Class[?]](classOf[Connection]), new InvocationHandler {
      override def invoke(proxy: AnyRef, method: Method, args: Array[AnyRef]): AnyRef = {
        method.getName match {
          case "commit" => counts.commits.incrementAndGet()
          case "rollback" if args == null => counts.rollbacks.incrementAndGet()
          case _ => ()
        }
        try method.invoke(raw, (if args == null then Array.empty[AnyRef] else args)*)
        catch { case e: InvocationTargetException => throw e.getCause }
      }
    }).asInstanceOf[Connection]

  class Manager(isolation: Option[String]) extends DataSourceConnectionManager {
    val counts = new Counts
    private lazy val pool: HikariDataSource = {
      val hc = new HikariConfig
      hc.setJdbcUrl(PostgreSQLTestSupport.jdbcUrl.get)
      hc.setUsername(PostgreSQLTestSupport.username)
      hc.setPassword(PostgreSQLTestSupport.password)
      hc.setAutoCommit(false)
      hc.setMaximumPoolSize(4)
      isolation.foreach(hc.setTransactionIsolation)
      new HikariDataSource(hc)
    }
    override protected def lazyBegin: Boolean = true
    override protected lazy val dataSource: DataSource =
      Proxy.newProxyInstance(getClass.getClassLoader, Array[Class[?]](classOf[DataSource]), new InvocationHandler {
        override def invoke(proxy: AnyRef, method: Method, args: Array[AnyRef]): AnyRef = method.getName match {
          case "getConnection" => counted(pool.getConnection, counts)
          case _ =>
            try method.invoke(pool, (if args == null then Array.empty[AnyRef] else args)*)
            catch { case e: InvocationTargetException => throw e.getCause }
        }
      }).asInstanceOf[DataSource]
    override protected def doDispose(): Task[Unit] = Task(pool.close())
  }

  class DB(isolation: Option[String] = None) extends LightDB {
    val manager = new Manager(isolation)
    override type SM = PostgreSQLStoreManager
    override val storeManager: PostgreSQLStoreManager = PostgreSQLStoreManager(manager)
    override def name: String = schema
    lazy val directory: Option[Path] = Some(Path.of(s"db/$schema"))
    val items: PostgreSQLStore[Item, Item.type] = store(Item)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  private def withJdbc[T](f: Connection => T): T = {
    val c = DriverManager.getConnection(PostgreSQLTestSupport.jdbcUrl.get, PostgreSQLTestSupport.username, PostgreSQLTestSupport.password)
    try f(c) finally c.close()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    if PostgreSQLTestSupport.jdbcUrl.isDefined then withJdbc { c =>
      val st = c.createStatement()
      try st.executeUpdate(s"""DROP SCHEMA IF EXISTS "$schema" CASCADE""")
      finally st.close()
    }
  }

  private lazy val db = new DB
  private def counts = db.manager.counts

  /** What reached the physical connections while `task` ran: (commits, rollbacks). */
  private def during[T](task: Task[T]): Task[(T, (Int, Int))] = Task.defer {
    val (c0, r0) = counts.snapshot
    task.map { result =>
      val (c1, r1) = counts.snapshot
      (result, (c1 - c0, r1 - r0))
    }
  }

  "Lazy begin on PostgreSQL" should {
    "initialize the database (schema creation commits its DDL)" in db.init.succeed
    "commit once for a transaction that wrote" in {
      during(db.items.transaction(_.insert((1 to 1500).toList.map(i => Item(s"item$i", i))))).map {
        case (_, (commits, rollbacks)) =>
          commits shouldBe 1
          rollbacks shouldBe 0
      }
    }
    "send nothing for a transaction that only read bounded results" in {
      during(db.items.transaction { tx =>
        for
          some <- tx.query.filter(_.n < 10).limit(20).toList
          one <- tx.get(some.head._id)
          count <- tx.count
          byName <- tx.query.filter(_.name === "item7").limit(1).toList
        yield (some.size, one.nonEmpty, count, byName.map(_.n))
      }).map {
        case (result, delta) =>
          result shouldBe ((9, true, 1500, List(7)))
          delta shouldBe ((0, 0))
      }
    }
    "stream a read larger than its fetch size inside a transaction" in {
      during(db.items.transaction(_.stream.count)).map {
        case (count, (commits, _)) =>
          count shouldBe 1500
          commits shouldBe 1
      }
    }
    "roll back a transaction that read and then wrote" in {
      for
        (result, (commits, rollbacks)) <- during(db.items.transaction { tx =>
          tx.count.flatMap(_ => tx.insert(Item("discarded", -1))).flatMap(_ => tx.flush)
            .flatMap(_ => Task.error[Unit](new RuntimeException("abort")))
        }.attempt)
        found <- db.items.transaction(_.query.filter(_.name === "discarded").count)
      yield {
        result.isFailure shouldBe true
        found shouldBe 0
        commits shouldBe 0
        rollbacks shouldBe 1
      }
    }
    "hold a locking read's lock until the commit" in {
      val target = "item42"
      def lockedElsewhere: Boolean = withJdbc { c =>
        c.setAutoCommit(false)
        val st = c.createStatement()
        try {
          st.executeQuery(s"""SELECT "_id" FROM "$schema"."Item" WHERE "name" = '$target' FOR UPDATE NOWAIT""").close()
          false
        } catch {
          case _: SQLException => true
        } finally {
          c.rollback()
          st.close()
        }
      }
      for
        (whileHeld, (commits, _)) <- during(db.items.transaction { tx =>
          Task {
            val c = tx.state.connectionManager.getConnection(tx.state)
            val st = c.createStatement()
            try st.executeQuery(s"""SELECT "_id" FROM "$schema"."Item" WHERE "name" = '$target' FOR UPDATE""").close()
            finally st.close()
            lockedElsewhere
          }
        })
      yield {
        whileHeld shouldBe true
        lockedElsewhere shouldBe false
        commits shouldBe 1
      }
    }
    "leave connections at REPEATABLE READ in manual commit" in {
      val rr = new DB(Some("TRANSACTION_REPEATABLE_READ"))
      for
        _ <- rr.init
        (count, commits) <- Task.defer {
          val c0 = rr.manager.counts.commits.get
          rr.items.transaction(_.count).map(n => (n, rr.manager.counts.commits.get - c0))
        }
        _ <- rr.dispose
      yield {
        count shouldBe 1500
        commits shouldBe 1
      }
    }
    "dispose" in db.dispose.succeed
  }
}
