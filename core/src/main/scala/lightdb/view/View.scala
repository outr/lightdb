package lightdb.view

import fabric.rw.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.field.Field
import lightdb.filter.*
import lightdb.store.Collection
import rapid.Task

import java.util.concurrent.{ConcurrentHashMap, Semaphore}
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A [[Relation]] materialized into a real, queryable [[Collection]] — a first-class view defined by
 * a (join-capable) view query. Phase 1: rebuilt on demand via [[refresh]] (run the relation through
 * the [[RelationEngine]], replace the contents). Phase 2 adds automatic incremental maintenance
 * driven by the dependencies derived from [[relation]].
 */
class View[Doc <: Document[Doc], Model <: DocumentModel[Doc]](val store: Collection[Doc, Model],
                                                             val relation: Relation,
                                                             val materialization: Materialization) {
  def model: Model = store.model

  /** The collections this view is derived from (auto-derived from the relation structure). */
  def dependencies: Set[lightdb.store.Store[?, ?]] = relation.dependencies

  /** Truncate and fully rebuild the view from its relation; returns the number of rows materialized. */
  def reBuild: Task[Int] = RelationEngine.execute(relation).flatMap { rows =>
    val docs = rows.map(_.as[Doc](store.model.rw))
    store.transaction(tx => tx.truncate.flatMap(_ => tx.upsert(docs)).map(_ => docs.size))
  }

  /** Query the materialized view like any collection. */
  def transaction[R](f: store.TX => Task[R]): Task[R] = store.transaction(f)

  private val maintainLock = new Semaphore(1)
  private val maintainDirty = new AtomicBoolean(false)

  /**
   * Serialized, coalescing maintenance pass. Every caller flags the view dirty and queues on the
   * lock; the single holder drains — rebuilding while dirty, then re-checking — so concurrent
   * dependency commits never interleave two rebuilds and a burst collapses into the minimum number
   * of rebuilds while still observing every committed change (each rebuild reads current state, and
   * a change flagged before the holder's `compareAndSet` is always picked up).
   */
  private def maintain(): Task[Unit] = Task.defer {
    maintainDirty.set(true)
    Task(maintainLock.acquire()).flatMap { _ =>
      def drain(): Task[Unit] =
        if (maintainDirty.compareAndSet(true, false)) reBuild.flatMap(_ => drain())
        else Task.unit
      drain().guarantee(Task(maintainLock.release()))
    }
  }

  /**
   * For a [[Materialization.Triggered]] view, subscribe to every dependency so the view is rebuilt
   * after any of them commits a change. Conservative correctness: each commit triggers a full
   * [[reBuild]] (always correct); a finer-grained, scope-derived incremental update is a later
   * precision optimization over the same dependency set. No-op for non-triggered materializations.
   *
   * Initial population (backfill of data already present before subscription) is the caller's
   * responsibility via [[reBuild]] after `db.init`; triggers keep the view current from then on.
   */
  private[view] def installTriggers(): Unit = materialization match {
    case Materialization.Triggered => maintainOn(dependencies.toSeq*)
    case _ => ()
  }

  /**
   * Subscribe the given stores so the view is rebuilt (coalescing, serialized) after any of them
   * commits a change. [[Materialization.Triggered]] auto-calls this for every dependency; callers
   * can invoke it explicitly to maintain a `Cached` view off a chosen subset of dependencies (e.g.
   * the frequently-changing one), leaving rarely-changing dependencies to a periodic [[reBuild]].
   */
  def maintainOn(stores: lightdb.store.Store[?, ?]*): Unit = stores.foreach(_.onCommittedChange(() => maintain()))

  private val scopedLock = new Semaphore(1)
  private val pendingScopes = new ConcurrentHashMap[Any, () => Task[Unit]]()
  private val needsFullRebuild = new AtomicBoolean(false)

  /**
   * Scope-incremental maintenance: instead of rebuilding the entire view after every dependency
   * commit, recompute only the partitions that changed. The view must be partitioned by `scopeField`
   * (a single indexed view column, e.g. `WatchSummary.profileId`) such that a change to a `dependency`
   * document with scope `s` can affect only view rows where `scopeField == s`, and `scopedRelation(s)`
   * produces exactly those rows. On commit, each inserted/upserted document's scope (`scopeOf`) is
   * recomputed: the view rows in that scope are deleted and replaced with the freshly executed scoped
   * relation (so an upsert that drops a row from the scope is handled). Deletes carry no document, so
   * any delete/truncate falls back to a full [[reBuild]] (always correct). Recomputes are serialized
   * and coalesced per scope, exactly like [[maintain]].
   *
   * Correctness rests on the caller's partition guarantee: rows in a scope depend only on dependency
   * documents of that scope, and a document's scope is stable across upserts (a partition *move* is not
   * observable from the new document alone, so it would strand the old scope's row). For relations that
   * don't partition cleanly (cross-partition joins/aggregates) or whose scope can change, use
   * [[maintainOn]] (full rebuild) instead.
   */
  def maintainScopedOn[D <: Document[D], DM <: DocumentModel[D], S](dependency: lightdb.store.Store[D, DM],
                                                                    scopeField: Field.Indexed[Doc, S])
                                                                   (scopeOf: D => S)
                                                                   (scopedRelation: S => Relation): Unit =
    dependency.onCommittedChangeDetailed { (docs, removed) =>
      Task.defer {
        if (removed) needsFullRebuild.set(true)
        docs.foreach { d =>
          val s = scopeOf(d)
          pendingScopes.put(s, () => recomputeScope(s, scopeField, scopedRelation))
        }
        drainScoped()
      }
    }

  /**
   * Scope-incremental maintenance that can also place a DELETE.
   *
   * [[maintainScopedOn]] falls back to a full rebuild on any removal, because a delete carries no
   * document to derive a scope from. Where the id encodes its scope, [[scopeOfId]] recovers it and the
   * removal is recomputed like any other change - turning "one row deleted" from a whole-view rebuild
   * into a single scope. Return None for an id whose scope cannot be read; that one falls back, so a
   * partial answer is still safe.
   *
   * Truncation always rebuilds: it removes everything, and no per-id scope survives to recompute.
   */
  def maintainScopedWithRemovals[D <: Document[D], DM <: DocumentModel[D], S](dependency: lightdb.store.Store[D, DM],
                                                                             scopeField: Field.Indexed[Doc, S])
                                                                            (scopeOf: D => S)
                                                                            (scopeOfId: Id[D] => Option[S])
                                                                            (scopedRelation: S => Relation): Unit =
    dependency.onCommittedChangeWithRemovals { (docs, removedIds, truncated) =>
      Task.defer {
        if (truncated) needsFullRebuild.set(true)
        docs.foreach { d =>
          val s = scopeOf(d)
          pendingScopes.put(s, () => recomputeScope(s, scopeField, scopedRelation))
        }
        removedIds.foreach { id =>
          scopeOfId(id) match {
            case Some(s) => pendingScopes.put(s, () => recomputeScope(s, scopeField, scopedRelation))
            case None => needsFullRebuild.set(true)
          }
        }
        drainScoped()
      }
    }

  private def recomputeScope[S](s: S,
                                scopeField: Field.Indexed[Doc, S],
                                scopedRelation: S => Relation): Task[Unit] =
    RelationEngine.execute(scopedRelation(s)).flatMap { rows =>
      val docs = rows.map(_.as[Doc](store.model.rw))
      store.transaction { tx =>
        tx.doDelete(tx.query.filter(_ => scopeField === s)).flatMap(_ => tx.upsert(docs)).map(_ => ())
      }
    }

  private def drainScoped(): Task[Unit] = Task.defer {
    Task(scopedLock.acquire()).flatMap { _ =>
      def loop(): Task[Unit] =
        if (needsFullRebuild.compareAndSet(true, false)) {
          pendingScopes.clear()
          reBuild.flatMap(_ => loop())
        } else {
          val it = pendingScopes.keySet().iterator()
          if (it.hasNext) {
            val key = it.next()
            val fn = pendingScopes.remove(key)
            (if (fn != null) fn() else Task.unit).flatMap(_ => loop())
          } else {
            Task.unit
          }
        }
      loop().guarantee(Task(scopedLock.release()))
    }
  }
}

object View {
  def apply[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: Collection[Doc, Model],
                                                              materialization: Materialization = Materialization.cachedManual)
                                                             (relation: Relation): View[Doc, Model] = {
    val view = new View(store, relation, materialization)
    view.installTriggers()
    view
  }
}
