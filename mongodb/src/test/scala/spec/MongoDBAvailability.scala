package spec

import org.scalatest.{AsyncTestSuite, FutureOutcome}

/**
 * Cancels (rather than fails) every test in a MongoDB-backed spec when no MongoDB is reachable —
 * neither a local mongod on :27017 nor Docker/Testcontainers. Mirrors [[ChronicleMapAvailability]].
 */
trait MongoDBAvailability extends AsyncTestSuite {
  abstract override def withFixture(test: NoArgAsyncTest): FutureOutcome =
    if MongoDBTestSupport.connectionString.isEmpty then
      FutureOutcome.canceled("MongoDB unavailable — no local mongod on :27017 and Docker/Testcontainers not available. Skipping.")
    else super.withFixture(test)
}
