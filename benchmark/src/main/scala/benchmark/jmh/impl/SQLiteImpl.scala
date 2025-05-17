package benchmark.jmh.impl

import org.apache.commons.io.FileUtils
import rapid.Task

import java.io.File
import java.sql.{Connection, DriverManager}
import scala.util.Random

case class SQLiteImpl() extends BenchmarkImplementation {
  private var path: File = _
  private var conn: Connection = _

  override def init: Task[Unit] = Task {
    path = new File("benchdb-sqlite")
    FileUtils.deleteDirectory(path)
    path.mkdirs()

    val dbFile = new File(path, "sqlite.db")
    conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile.getAbsolutePath)
    conn.setAutoCommit(false)
    val stmt = conn.createStatement()
    stmt.executeUpdate("CREATE TABLE record (id TEXT PRIMARY KEY, key TEXT, number INTEGER)")
    stmt.close()
  }

  override def insert(iterations: Int): Task[Unit] = Task {
    val ps = conn.prepareStatement("INSERT INTO record(id, key, number) VALUES (?, ?, ?)")
    (0 until iterations).foreach { _ =>
      val id = Random.alphanumeric.take(12).mkString
      val key = Random.alphanumeric.take(24).mkString
      val num = Random.nextInt()
      ps.setString(1, id)
      ps.setString(2, key)
      ps.setInt(3, num)
      ps.addBatch()
    }
    ps.executeBatch()
    conn.commit()
  }

  override def count: Task[Int] = Task {
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT COUNT(*) FROM record")
    rs.next()
    val count = rs.getInt(1)
    rs.close()
    stmt.close()
    count
  }

  override def read: Task[Int] = Task {
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT id, key, number FROM record")
    var count = 0
    while (rs.next()) {
      val id = rs.getString("id")
      val key = rs.getString("key")
      val number = rs.getInt("number")
      require(id != null && key != null)
      count += 1
    }
    rs.close()
    stmt.close()
    count
  }

  override def traverse: Task[Unit] = Task.error(new UnsupportedOperationException("Traversal not supported in SQLiteImpl"))

  override def dispose: Task[Unit] = Task {
    conn.close()
    FileUtils.deleteDirectory(path)
  }
}
