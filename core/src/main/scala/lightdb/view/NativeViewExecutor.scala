package lightdb.view

import fabric.Json
import rapid.Task

/**
 * A backend's native execution of a [[Relation]], overriding the portable in-memory
 * [[RelationEngine]]. A store provides one (see `Store.nativeViewExecutor`) when it can run a whole
 * relation itself — e.g. a SQL store lowering the relation to a single JOIN so the database executes
 * it with its indexes instead of streaming whole collections into memory.
 *
 * The engine only uses native execution when every dependency of a relation shares the same
 * [[coLocationKey]] (e.g. one SQL database/connection), since a cross-collection join can only run
 * natively where the collections are co-located. Otherwise, and whenever [[execute]] returns `None`
 * (an unsupported relation shape), the engine falls back to the generic in-memory path — so results
 * are identical regardless of which executor runs.
 */
trait NativeViewExecutor {
  /** Identity of the backing database/connection; relations whose dependencies all share one key are
    * co-located and thus eligible for native execution. */
  def coLocationKey: AnyRef

  /** Execute the relation natively, or `None` to defer to the generic engine. */
  def execute(relation: Relation): Option[Task[List[Json]]]
}
