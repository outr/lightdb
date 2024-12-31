package lightdb.sql.connect

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import rapid.Task

case class HikariConnectionManager(config: SQLConfig) extends DataSourceConnectionManager {
  protected lazy val dataSource: HikariDataSource = {
    val hc = new HikariConfig
    hc.setJdbcUrl(config.jdbcUrl)
    config.driverClassName.foreach(hc.setDriverClassName)
    config.username.foreach(hc.setUsername)
    config.password.foreach(hc.setPassword)
    config.maximumPoolSize.foreach(hc.setMaximumPoolSize)
    hc.setAutoCommit(config.autoCommit)
    hc.addDataSourceProperty("cachePrepStmts", "true")
    hc.addDataSourceProperty("prepStmtCacheSize", "250")
    hc.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
    new HikariDataSource(hc)
  }

  override def dispose(): Task[Unit] = Task(dataSource.close())
}
