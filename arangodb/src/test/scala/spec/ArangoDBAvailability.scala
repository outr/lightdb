package spec

import org.scalatest.{AsyncTestSuite, FutureOutcome}

/**
 * Cancels (rather than fails) every test in an ArangoDB-backed spec when ArangoDB isn't reachable
 * (Docker/Testcontainers unavailable). Mirrors [[MongoDBAvailability]].
 */
trait ArangoDBAvailability extends AsyncTestSuite {
  abstract override def withFixture(test: NoArgAsyncTest): FutureOutcome =
    if ArangoDBTestSupport.config.isEmpty then
      FutureOutcome.canceled("ArangoDB unavailable — Docker/Testcontainers not available. Skipping.")
    else super.withFixture(test)
}
