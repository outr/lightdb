package spec

import org.scalatest.{AsyncTestSuite, FutureOutcome}

/**
 * Cancels (rather than fails) every test in a Qdrant-backed spec when no Qdrant is reachable
 * (Docker/Testcontainers unavailable). Mirrors [[MongoDBAvailability]].
 */
trait QdrantAvailability extends AsyncTestSuite {
  abstract override def withFixture(test: NoArgAsyncTest): FutureOutcome =
    if QdrantTestSupport.config.isEmpty then
      FutureOutcome.canceled("Qdrant unavailable — Docker/Testcontainers not available. Skipping.")
    else super.withFixture(test)
}
