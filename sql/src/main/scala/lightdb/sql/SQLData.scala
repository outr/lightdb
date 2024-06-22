//package lightdb.sql
//
//import cats.effect.IO
//import lightdb.{Document, Id}
//
//case class SQLData[D <: Document[D]](ids: List[Id[D]], lookup: Option[Id[D] => IO[D]])
