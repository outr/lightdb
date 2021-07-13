package benchmark

import cats.effect.IO

trait BenchmarkImplementation {
  type TitleAka
  type TitleBasics

  def name: String

  def init(): IO[Unit] = IO.unit

  def map2TitleAka(map: Map[String, String]): TitleAka
  def map2TitleBasics(map: Map[String, String]): TitleBasics

  def persistTitleAka(t: TitleAka): IO[Unit]
  def persistTitleBasics(t: TitleBasics): IO[Unit]

  def flush(): IO[Unit]

  def idFor(t: TitleAka): String
  def titleIdFor(t: TitleAka): String

  def streamTitleAka(): fs2.Stream[IO, TitleAka]
  def verifyTitleAka(): IO[Unit]
  def verifyTitleBasics(): IO[Unit]

  def get(id: String): IO[TitleAka]

  implicit class MapExtras(map: Map[String, String]) {
    def option(key: String): Option[String] = map.get(key) match {
      case Some("") | None => None
      case Some(s) => Some(s)
    }
    def value(key: String): String = option(key).getOrElse(throw new RuntimeException(s"Key not found: $key in ${map.keySet}"))
    def int(key: String, default: Int = 0): Int = option(key).map(_.toInt).getOrElse(default)
    def list(key: String): List[String] = option(key).map(_.split(' ').toList).getOrElse(Nil)
    def bool(key: String): Boolean = if (int(key) == 0) false else true
    def boolOption(key: String): Option[Boolean] = option(key).map(s => if (s.toInt == 0) false else true)
  }
}
