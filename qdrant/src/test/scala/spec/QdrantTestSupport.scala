package spec

import lightdb.qdrant.{QdrantConfig, QdrantStoreManager}

/**
 * Resolves a Qdrant connection for tests via an ephemeral Testcontainers instance (auto-removed at
 * JVM exit). Yields `None` when Docker is unavailable so specs cancel gracefully (see
 * [[QdrantAvailability]]).
 */
object QdrantTestSupport {
  lazy val config: Option[QdrantConfig] =
    try Some(QdrantConfig(QdrantTestContainer.host, QdrantTestContainer.grpcPort))
    catch {
      case t: Throwable =>
        scribe.warn(s"Qdrant unavailable: Testcontainers failed to start (${t.getMessage})")
        None
    }

  def storeManager: QdrantStoreManager = QdrantStoreManager(config.get)
}
