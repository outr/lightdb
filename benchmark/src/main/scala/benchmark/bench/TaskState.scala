package benchmark.bench

case class TaskState(name: String, maxProgress: Double = 1.0, f: StatusCallback => Int)