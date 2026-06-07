package spec

import lightdb.arangodb.ArangoDBConfig

/**
 * Resolves an ArangoDB connection for tests. ArangoDB has no predictable unauthenticated local
 * default, so (unlike MongoDB/PostgreSQL) there's no local fast path — tests always use an ephemeral
 * Testcontainers ArangoDB, and cancel gracefully when Docker is unavailable (see
 * [[ArangoDBAvailability]]).
 */
object ArangoDBTestSupport {
  lazy val config: Option[ArangoDBConfig] =
    try Some(ArangoDBTestContainer.config)
    catch {
      case t: Throwable =>
        scribe.warn(s"ArangoDB unavailable: Testcontainers failed to start (${t.getMessage})")
        None
    }
}
