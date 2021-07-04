package benchmark

import scala.concurrent.{ExecutionContext, Future}

trait BenchmarkImplementation {
  type TitleAka

  def name: String

  def init()(implicit ec: ExecutionContext): Future[Unit] = Future.unit

  def map2TitleAka(map: Map[String, String]): TitleAka
  def persistTitleAka(t: TitleAka)(implicit ec: ExecutionContext): Future[Unit]

  def flush()(implicit ec: ExecutionContext): Future[Unit]

  def verifyTitleAka()(implicit ec: ExecutionContext): Future[Unit]

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
