package benchmark.bench

import benchmark.ActionIterator
import lightdb.{Id, Unique}

import java.sql.ResultSet
import scala.collection.parallel.CollectionConverters._

trait Bench {
//  val RecordCount: Int = 500_000
  val RecordCount: Int = 10_000_000
  val StreamAsyncIterations: Int = 8
  val GetAsyncIterations: Int = 8
  val SearchEachAsyncIterations: Int = 8
  val SearchAllAsyncIterations: Int = 8
  val SearchIterations: Int = 1

  val tasks: List[Task] = List(
    Task("Insert Records", RecordCount, insertRecordsTask),
    Task("Stream Records", RecordCount, streamRecordsTask),
    Task("Stream Records Multi", RecordCount * StreamAsyncIterations, streamRecordsAsyncTask),
    Task("Get Each Record", RecordCount, getEachRecordTask),
    Task("Get Each Record Multi", RecordCount * GetAsyncIterations, getEachRecordAsyncTask),
    Task("Search Each Record", RecordCount, searchEachRecordTask),
    Task("Search Each Record Multi", RecordCount * SearchEachAsyncIterations, searchEachRecordAsyncTask),
    Task("Search All Records", RecordCount, searchAllRecordsTask),
    Task("Search All Records Multi", RecordCount, searchAllRecordsAsyncTask)
  )

  def name: String

  def init(): Unit

  private def insertRecordsTask(status: StatusCallback): Int = {
    val iterator = ActionIterator(
      (0 until RecordCount).iterator.map(index => P(Unique(), index, s"id$index")),
      b => if (b) status.progress()
    )
    insertRecords(iterator)
    status.currentProgress
  }

  private def streamRecordsTask(status: StatusCallback): Int = {
    streamRecords { iterator =>
      iterator.foreach { p =>
        status.progress()
      }
    }
    status.currentProgress
  }

  private def streamRecordsAsyncTask(status: StatusCallback): Int = {
    (0 until StreamAsyncIterations)
      .par
      .foldLeft(0)((total, _) => total + streamRecordsTask(status))
  }

  private def getEachRecordTask(status: StatusCallback): Int = {
    val iterator = ActionIterator(
      (0 until RecordCount).iterator.map(index => s"id$index"),
      b => if (b) status.progress()
    )
    getEachRecord(iterator)
    status.currentProgress
  }

  private def getEachRecordAsyncTask(status: StatusCallback): Int = {
    (0 until GetAsyncIterations)
      .par
      .foldLeft(0)((total, _) => total + getEachRecordTask(status))
  }

  private def searchEachRecordTask(status: StatusCallback): Int = {
    val iterator = ActionIterator(
      (0 until RecordCount).iterator,
      b => if (b) status.progress()
    )
    searchEachRecord(iterator)
    status.currentProgress
  }

  private def searchEachRecordAsyncTask(status: StatusCallback): Int = {
    (0 until SearchEachAsyncIterations)
      .par
      .foldLeft(0)((total, _) => total + searchEachRecordTask(status))
  }

  private def searchAllRecordsTask(status: StatusCallback): Int = {
    searchAllRecords { iterator =>
      iterator.foreach { p =>
        status.progress()
      }
    }
    status.currentProgress
  }

  private def searchAllRecordsAsyncTask(status: StatusCallback): Int = {
    (0 until SearchAllAsyncIterations)
      .par
      .foldLeft(0)((total, _) => total + searchAllRecordsTask(status))
  }

  protected def insertRecords(iterator: Iterator[P]): Unit

  protected def streamRecords(f: Iterator[P] => Unit): Unit

  protected def getEachRecord(idIterator: Iterator[String]): Unit

  protected def searchEachRecord(ageIterator: Iterator[Int]): Unit

  protected def searchAllRecords(f: Iterator[P] => Unit): Unit

  def size(): Long

  def dispose(): Unit

  case class P(name: String, age: Int, id: String)

  def rsIterator(rs: ResultSet): Iterator[P] = new Iterator[P] {
    override def hasNext: Boolean = rs.next()

    override def next(): P = {
      P(
        name = rs.getString("name"),
        age = rs.getInt("age"),
        id = rs.getString("id")
      )
    }
  }
}
