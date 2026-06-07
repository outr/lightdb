package spec

import org.scalatest.{AsyncTestSuite, FutureOutcome}

/**
 * Cancels (rather than fails) every test in a pgvector-backed spec when no pgvector container can be
 * started (Docker/Testcontainers unavailable). Mirrors [[PostgreSQLAvailability]].
 */
trait PgVectorAvailability extends AsyncTestSuite {
  abstract override def withFixture(test: NoArgAsyncTest): FutureOutcome =
    if PgVectorTestSupport.jdbcUrl.isEmpty then
      FutureOutcome.canceled("pgvector unavailable — Docker/Testcontainers not available. Skipping.")
    else super.withFixture(test)
}
