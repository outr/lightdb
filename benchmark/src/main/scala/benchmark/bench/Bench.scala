package benchmark.bench

trait Bench {
  val RecordCount: Int = 1_000_000
  val StreamIterations: Int = 1
  val SearchIterations: Int = 1

  val tasks: List[Task] = List(
    Task("Insert Records", RecordCount, insertRecords),
    Task("Stream Records", StreamIterations, streamRecords),
    Task("Search Each Record", StreamIterations * RecordCount, searchEachRecord),
    Task("Search All Records", StreamIterations, searchAllRecords)
  )

  def init(): Unit

  protected def insertRecords(status: StatusCallback): Unit

  protected def streamRecords(status: StatusCallback): Unit

  protected def searchEachRecord(status: StatusCallback): Unit

  protected def searchAllRecords(status: StatusCallback): Unit

  def dispose(): Unit
}
