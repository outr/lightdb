package lightdb.sql.connect

case class SQLConfig(jdbcUrl: String,
                     driverClassName: Option[String] = None,
                     username: Option[String] = None,
                     password: Option[String] = None,
                     minimumIdle: Option[Int] = None,
                     maximumPoolSize: Option[Int] = None,
                     autoCommit: Boolean = false,
                     // SQL run on each new pooled connection. Defaults to PostgreSQL session tuning;
                     // dialects with different session syntax (e.g. MySQL/MariaDB) override it.
                     connectionInitSql: Option[String] = Some("SET statement_timeout = 0; SET idle_in_transaction_session_timeout = 0;"))
