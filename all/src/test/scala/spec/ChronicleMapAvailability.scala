package spec

import org.scalatest.{AsyncTestSuite, FutureOutcome}

/**
 * Cancels (rather than aborts) every test in a ChronicleMap-backed spec when the JVM lacks
 * `jdk.internal.ref.Cleaner`.
 *
 * Mirror of the trait in the `chronicleMap` module's test sources — duplicated here because
 * the `all` module doesn't pull in `chronicleMap`'s test scope (`% "test->test"` is not
 * declared). See the chronicleMap copy for the full rationale.
 */
trait ChronicleMapAvailability extends AsyncTestSuite {
  private lazy val chronicleMapAvailable: Boolean =
    try { Class.forName("jdk.internal.ref.Cleaner"); true }
    catch { case _: ClassNotFoundException => false }

  abstract override def withFixture(test: NoArgAsyncTest): FutureOutcome =
    if !chronicleMapAvailable then
      FutureOutcome.canceled("ChronicleMap requires jdk.internal.ref.Cleaner — removed in JDK 24+. Skipping on this JVM.")
    else super.withFixture(test)
}
