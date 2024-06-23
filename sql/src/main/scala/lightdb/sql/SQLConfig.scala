package lightdb.sql

case class SQLConfig(jdbcUrl: String,
                     username: Option[String] = None,
                     password: Option[String] = None,
                     maximumPoolSize: Option[Int] = None)
