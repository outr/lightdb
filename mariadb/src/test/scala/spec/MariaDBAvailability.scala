package spec

import org.scalatest.{AsyncTestSuite, FutureOutcome}

/**
 * Cancels (rather than fails) every test in a MariaDB-backed spec when no MariaDB/MySQL is reachable
 * — neither a local server on :3306 nor Docker/Testcontainers. Mirrors [[PostgreSQLAvailability]].
 */
trait MariaDBAvailability extends AsyncTestSuite {
  abstract override def withFixture(test: NoArgAsyncTest): FutureOutcome =
    if MariaDBTestSupport.jdbcUrl.isEmpty then
      FutureOutcome.canceled("MariaDB unavailable — no local server on :3306 and Docker/Testcontainers not available. Skipping.")
    else super.withFixture(test)
}
