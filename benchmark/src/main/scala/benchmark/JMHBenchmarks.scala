//package benchmark
//
//import cats.effect.unsafe.implicits.global
//import fabric.rw._
//import lightdb.collection.Collection
//import lightdb._
//import lightdb.document.{Document, DocumentModel}
//import lightdb.halo.HaloDBStore
//import lightdb.store.{AtomicMapStore, StoreManager}
//import lightdb.upgrade.DatabaseUpgrade
//import lightdb.util.Unique
//import org.apache.commons.io.FileUtils
//import org.openjdk.jmh.annotations
//
//import java.io.File
//import java.nio.file.Path
//import java.sql.DriverManager
//import java.util.concurrent.TimeUnit
//import scala.util.Random
//
//@annotations.State(annotations.Scope.Thread)
//class JMHBenchmarks {
//  private val Iterations: Int = 1000
//
//  private lazy val sqliteConnection = {
//    val c = DriverManager.getConnection("jdbc:sqlite:db/sqlite.db")
//    c.setAutoCommit(false)
//    c
//  }
//
//  @annotations.Setup(annotations.Level.Trial)
//  def doSetup(): Unit = {
//    val dbDir = new File("db")
//    new File("benchmarks.json").delete()
//    FileUtils.deleteDirectory(dbDir)
//    dbDir.mkdirs()
//    db.init
//
//    val s = sqliteConnection.createStatement()
//    s.executeUpdate("CREATE TABLE record(id VARCHAR NOT NULL, key TEXT, number INTEGER, PRIMARY KEY (id))")
//    s.close()
//    sqliteConnection.commit()
//  }
//
//  @annotations.Benchmark
//  @annotations.BenchmarkMode(Array(annotations.Mode.Throughput))
//  @annotations.OutputTimeUnit(TimeUnit.MILLISECONDS)
//  @annotations.OperationsPerInvocation(1000)
//  def records1InsertLightDB(): Unit = DB.records.transaction { transaction =>
//    val records = (0 until Iterations).map(_ => Record.generate())
//    DB.records.set(records)
//  }
//
//  @annotations.Benchmark
//  @annotations.BenchmarkMode(Array(annotations.Mode.Throughput))
//  @annotations.OutputTimeUnit(TimeUnit.MILLISECONDS)
//  @annotations.OperationsPerInvocation(1000)
//  def records2CountLightDB(): Unit = DB.records.transaction { transaction =>
//    val count = DB.records.count
//    scribe.info(s"LightDB Count: $count")
//  }
//
//  @annotations.Benchmark
//  @annotations.BenchmarkMode(Array(annotations.Mode.Throughput))
//  @annotations.OutputTimeUnit(TimeUnit.MILLISECONDS)
//  @annotations.OperationsPerInvocation(1000)
//  def records3ReadLightDB(): Unit = DB.records.transaction { transaction =>
//    val total = DB.records.iterator.map(_.number).fold(0)((total, _) => total + 1)
//    scribe.info(s"LightDB Total: $total")
//  }
//
//  @annotations.Benchmark
//  @annotations.BenchmarkMode(Array(annotations.Mode.Throughput))
//  @annotations.OutputTimeUnit(TimeUnit.MILLISECONDS)
//  @annotations.OperationsPerInvocation(1000)
//  def records4InsertSQLite(): Unit = {
//    val records = (0 until Iterations).map(_ => Record.generate())
//    val ps = sqliteConnection.prepareStatement("INSERT INTO record(id, key, number) VALUES (?, ?, ?)")
//    records.foreach { r =>
//      ps.setString(1, r._id.value)
//      ps.setString(2, r.key)
//      ps.setInt(3, r.number)
//      ps.addBatch()
//    }
//    ps.executeBatch()
//    sqliteConnection.commit()
//  }
//
//  @annotations.Benchmark
//  @annotations.BenchmarkMode(Array(annotations.Mode.Throughput))
//  @annotations.OutputTimeUnit(TimeUnit.MILLISECONDS)
//  @annotations.OperationsPerInvocation(1000)
//  def records5CountSQLite(): Unit = {
//    val s = sqliteConnection.createStatement()
//    val rs = s.executeQuery("SELECT COUNT(id) FROM record")
//    rs.next()
//    val count = rs.getInt(1)
//    scribe.info(s"SQLite Count: $count")
//  }
//
//  @annotations.Benchmark
//  @annotations.BenchmarkMode(Array(annotations.Mode.Throughput))
//  @annotations.OutputTimeUnit(TimeUnit.MILLISECONDS)
//  @annotations.OperationsPerInvocation(1000)
//  def records6ReadSQLite(): Unit = {
//    val s = sqliteConnection.createStatement()
//    val rs = s.executeQuery("SELECT id, key, number FROM record")
//    var total = 0
//    while (rs.next()) {
//      val record = Record(
//        key = rs.getString(2),
//        number = rs.getInt(3),
//        _id = Record.id(rs.getString(1))
//      )
//      total += 1
//    }
//    rs.close()
//    s.close()
//    scribe.info(s"SQLite Total: $total")
//  }
//
//  object DB extends LightDB {
//    override def directory: Option[Path] = Some(Path.of("db/jmh"))
//
//    val records: Collection[Record, Record.type] = collection("records", Record)
//
//    override def storeManager: StoreManager = HaloDBStore
//
//    override def upgrades: List[DatabaseUpgrade] = Nil
//  }
//
//  case class Record(key: String, number: Int, _id: Id[Record] = Record.id()) extends Document[Record]
//
//  object Record extends DocumentModel[Record] {
//    override implicit val rw: RW[Record] = RW.gen
//
//    def generate(): Record = Record(
//      key = Unique(),
//      number = Random.nextInt()
//    )
//  }
//}