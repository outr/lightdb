package lightdb.sql.connect

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import rapid.Task

import scala.concurrent.duration.DurationInt

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
    hc.setMaximumPoolSize(8)
    hc.setMinimumIdle(2)
    hc.setIdleTimeout(60.seconds.toMillis)
    hc.setConnectionTimeout(30.seconds.toMillis)
    new HikariDataSource(hc)
  }

  override protected def doDispose(): Task[Unit] = Task.unit
}
