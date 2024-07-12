package lightdb.sql.connect

case class SQLConfig(jdbcUrl: String,
                     driverClassName: Option[String] = None,
                     username: Option[String] = None,
                     password: Option[String] = None,
                     minimumIdle: Option[Int] = None,
                     maximumPoolSize: Option[Int] = None,
                     autoCommit: Boolean = false)
