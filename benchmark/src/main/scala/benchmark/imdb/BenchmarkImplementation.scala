package benchmark.imdb

import rapid.Task

trait BenchmarkImplementation {
  type TitleAka
  type TitleBasics

  def name: String

  def init(): Task[Unit] = Task.unit

  def map2TitleAka(map: Map[String, String]): TitleAka
  def map2TitleBasics(map: Map[String, String]): TitleBasics

  def persistTitleAka(t: TitleAka): Task[Unit]
  def persistTitleBasics(t: TitleBasics): Task[Unit]

  def flush(): Task[Unit]

  def idFor(t: TitleAka): String
  def titleIdFor(t: TitleAka): String

  def streamTitleAka(): rapid.Stream[TitleAka]
  def verifyTitleAka(): Task[Unit]
  def verifyTitleBasics(): Task[Unit]

  def get(id: String): Task[TitleAka]
  def findByTitleId(titleId: String): Task[List[TitleAka]]

  private val excludeChars = Set(2.toChar)

  implicit class MapExtras(map: Map[String, String]) {
    def option(key: String): Option[String] = map.get(key) match {
      case Some("") | None => None
      case Some(s) => Some(filtered(s))
    }
    def value(key: String): String = option(key).getOrElse("")
    def int(key: String, default: Int = 0): Int = option(key).map(_.toInt).getOrElse(default)
    def list(key: String): List[String] = option(key).map(_.split(' ').toList).getOrElse(Nil)
    def bool(key: String): Boolean = if (int(key) == 0) false else true
    def boolOption(key: String): Option[Boolean] = option(key).map(s => if (s.toInt == 0) false else true)

    private def filtered(s: String): String = s.filterNot(excludeChars.contains)
  }
}
