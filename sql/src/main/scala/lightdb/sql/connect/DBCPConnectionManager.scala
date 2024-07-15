package lightdb.sql.connect

import org.apache.commons.dbcp2.BasicDataSource

case class DBCPConnectionManager(config: SQLConfig) extends DataSourceConnectionManager {
  protected lazy val dataSource: BasicDataSource = {
    val ds = new BasicDataSource
    ds.setUrl(config.jdbcUrl)
    config.username.foreach(ds.setUsername)
    config.password.foreach(ds.setPassword)
    config.minimumIdle.foreach(ds.setMinIdle)
    config.maximumPoolSize.foreach(ds.setMaxTotal)
    ds.setDefaultAutoCommit(false)
    ds.setAutoCommitOnReturn(false)
    ds.setPoolPreparedStatements(true)
    ds.setMaxOpenPreparedStatements(1_000_000)
    ds
  }

  override def dispose(): Unit = dataSource.close()
}

