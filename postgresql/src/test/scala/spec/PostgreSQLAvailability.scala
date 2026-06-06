package spec

import org.scalatest.{AsyncTestSuite, FutureOutcome}

/**
 * Cancels (rather than fails) every test in a PostgreSQL-backed spec when no PostgreSQL is reachable
 * — neither a local server on :5432 nor Docker/Testcontainers. Mirrors [[MongoDBAvailability]].
 */
trait PostgreSQLAvailability extends AsyncTestSuite {
  abstract override def withFixture(test: NoArgAsyncTest): FutureOutcome =
    if PostgreSQLTestSupport.jdbcUrl.isEmpty then
      FutureOutcome.canceled("PostgreSQL unavailable — no local server on :5432 and Docker/Testcontainers not available. Skipping.")
    else super.withFixture(test)
}
