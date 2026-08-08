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
    hc.setAutoCommit(config.autoCommit)
    hc.addDataSourceProperty("cachePrepStmts", "true")
    hc.addDataSourceProperty("prepStmtCacheSize", "250")
    hc.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
    // CONFIGURED VALUES WIN. Both were previously ignored: maximumPoolSize was applied and then
    // overwritten by an unconditional 128 on the next line, and minimumIdle was never read at all --
    // it was hardcoded to 2 whatever the caller asked for.
    //
    // minimumIdle matters more than it looks. At 2, a bursty workload pays connection ESTABLISHMENT
    // on nearly every checkout: measured against a database answering in under 3ms, each query cost
    // ~30ms end to end, and a request making ~30 queries spent most of its time connecting rather
    // than querying.
    hc.setMaximumPoolSize(config.maximumPoolSize.getOrElse(128))
    hc.setMinimumIdle(config.minimumIdle.getOrElse(2))
    hc.setIdleTimeout(60.seconds.toMillis)
    hc.setConnectionTimeout(5.minutes.toMillis)
    config.connectionInitSql.foreach(hc.setConnectionInitSql)
    hc.setLeakDetectionThreshold(if HikariConnectionManager.EnableLeakDetection then 5.minutes.toMillis else 1.hour.toMillis)
    new HikariDataSource(hc)
  }

  override protected def doDispose(): Task[Unit] = Task.unit
}

object HikariConnectionManager {
  val EnableLeakDetection = false
}