package spec

import org.scalatest.{AsyncTestSuite, FutureOutcome}

/**
 * Cancels (rather than aborts) every test in a ChronicleMap-backed spec when the JVM lacks
 * `jdk.internal.ref.Cleaner`.
 *
 * ChronicleMap's `CleanerUtils` static initializer calls `Class.forName("jdk.internal.ref.Cleaner")`;
 * that class was removed in JDK 24+. ChronicleMap v2026.1 still throws here, which without this
 * gate surfaces as a suite-level `*** ABORTED ***` (treated as failure by sbt test) — not a
 * clean per-test cancellation. CI runs on JDK 23 where the class still exists, so this only
 * matters on developer machines and any future CI image that moves past JDK 23.
 *
 * Mix in alongside `AsyncTaskSpec` / `AsyncWordSpec`; tests run normally when the class is
 * present and are reported as canceled (not failed/aborted) when it isn't.
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
