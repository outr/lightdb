package benchmark.bench

trait Bench {
  val RecordCount: Int = 10_000_000
  val StreamIterations: Int = 1
  val SearchIterations: Int = 1

  val tasks: List[Task] = List(
    Task("Insert Records", RecordCount, insertRecords),
    Task("Stream Records", StreamIterations * RecordCount, streamRecords),
    Task("Search Each Record", StreamIterations * RecordCount, searchEachRecord),
    Task("Search All Records", StreamIterations * RecordCount, searchAllRecords)
  )

  def name: String

  def init(): Unit

  protected def insertRecords(status: StatusCallback): Int

  protected def streamRecords(status: StatusCallback): Int

  protected def searchEachRecord(status: StatusCallback): Int

  protected def searchAllRecords(status: StatusCallback): Int

  def size(): Long

  def dispose(): Unit
}
