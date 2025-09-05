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
    hc.setMaximumPoolSize(128)
    hc.setMinimumIdle(2)
    hc.setIdleTimeout(60.seconds.toMillis)
    hc.setConnectionTimeout(5.minutes.toMillis)
    hc.setConnectionInitSql("SET statement_timeout = 0; SET idle_in_transaction_session_timeout = 0;")
    hc.setLeakDetectionThreshold(if (HikariConnectionManager.EnableLeakDetection) 5.minutes.toMillis else 1.hour.toMillis)
    new HikariDataSource(hc)
  }

  override protected def doDispose(): Task[Unit] = Task.unit
}

object HikariConnectionManager {
  val EnableLeakDetection = false
}