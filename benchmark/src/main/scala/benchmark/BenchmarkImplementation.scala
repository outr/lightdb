package benchmark

import cats.effect.IO

trait BenchmarkImplementation {
  type TitleAka

  def name: String

  def init(): IO[Unit] = IO.unit

  def map2TitleAka(map: Map[String, String]): TitleAka
  def persistTitleAka(t: TitleAka): IO[Unit]

  def flush(): IO[Unit]

  def streamTitleAka(): fs2.Stream[IO, TitleAka]
  def verifyTitleAka(): IO[Unit]

  implicit class MapExtras(map: Map[String, String]) {
    def option(key: String): Option[String] = map.get(key) match {
      case Some("") | None => None
      case Some(s) => Some(s)
    }
    def value(key: String): String = option(key).getOrElse(throw new RuntimeException(s"Key not found: $key in ${map.keySet}"))
    def int(key: String): Int = value(key).toInt
    def list(key: String): List[String] = option(key).map(_.split(' ').toList).getOrElse(Nil)
    def bool(key: String): Boolean = if (int(key) == 0) false else true
    def boolOption(key: String): Option[Boolean] = option(key).map(s => if (s.toInt == 0) false else true)
  }
}
