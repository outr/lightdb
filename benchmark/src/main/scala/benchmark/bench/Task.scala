package benchmark.bench

case class Task(name: String, maxProgress: Double = 1.0, f: StatusCallback => Int)