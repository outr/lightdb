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
                     // The tcp_keepalives_* SETs make the server probe each client socket so a crashed
                     // client's backend (and its locks) is reaped instead of lingering idle-in-transaction;
                     // they apply to TCP connections and are harmless no-ops on Unix-socket connections.
                     connectionInitSql: Option[String] = Some("SET statement_timeout = 0; SET idle_in_transaction_session_timeout = 0; SET tcp_keepalives_idle = 60; SET tcp_keepalives_interval = 10; SET tcp_keepalives_count = 6;"))
