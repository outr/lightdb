# Transaction failure safety: review notes

This change repairs the managed transaction lifecycle. It does not turn nontransactional stores into
transactional databases or provide distributed transactions. Rebuild dependent modules together:
`rollback` now lives on `Transaction`; `RollbackSupport` remains a source-compatible marker.

## Managed contract

- `store.transaction(body)` commits only after a successful body and transaction-end hook.
- A failed body, synchronous body construction, start hook, end hook, or commit attempts rollback,
  writer shutdown and resource cleanup. The original error remains primary; cleanup errors are
  suppressed. Active-transaction bookkeeping and rollback listeners are cleaned up on failure.
- Explicit `tx.rollback` is terminal: managed release cannot commit it again. Manual acquisition must
  be paired with `store.transaction.release(tx)` for success or `store.transaction.abort(tx)` for failure.
- Commit observers run after commit and resource closure. All observers are attempted even if one
  fails. Their errors propagate but do not undo durable data: blindly retrying such a failure is unsafe.
- Database disposal aborts active transactions instead of implicitly committing unfinished bodies.
  Applications must still stop and join their producers before disposing stores.
- SQLState is the only SQL commit path. JDBC commit errors propagate. SQL close discards batches and
  closes statements/results; datasource release rolls back uncommitted work and closes, never commits.
- Async flush includes writes already dequeued by workers. Abort discards queued writes and joins
  workers before backend rollback. Zero workers are rejected, and a barrier detects exited workers.
- TransactionManager brackets acquisition incrementally, aborting open scopes after a later failure.
  MultiStore attempts cleanup of every acquired shard. Split/traversal wrappers forward rollback.
- A failed shared-transaction body discards its entire uncommitted batch; it is not returned to the pool
  for later commit. Failed initialization/acquisition wakes waiters or releases reserved capacity.

## Tests

From this checkout, using sbt 2 (commands separated inside one argument):

```sh
sbt -batch 'sqlite/testOnly spec.SQLiteTransactionFailureSpec spec.SQLiteReadYourWritesSpec spec.SQLiteStoreCacheSpec spec.SQLiteScopedViewRemovalSpec; sql/testOnly spec.SQLTransactionCleanupSpec; core/testOnly spec.AsyncWriteHandlerErrorPropagationSpec spec.AsyncWriteBarrierSpec; all/compile'
```

46 tests in seven suites passed on 2026-09-06: 16 lifecycle/failure tests, seven read-your-writes,
10 cache, five scoped-view-removal, four JDBC cleanup, one async error-propagation and three async
barrier/worker-validation tests. JDBC cleanup tests inject commit and rollback failures without an
external service. SQLite tests use isolated in-memory fixtures. `all/compile` and aggregate
`publishLocal` also succeeded; publication was local only. This is not a full backend matrix.

The Nabo consumer adds disposable PostgreSQL tests of the ordinary managed API, including all five
batch modes, failed bodies before/after explicit flush, a deferred constraint failing at commit,
connection reuse, explicit rollback and materialized-view behavior. See its hardening tracker for
the consumer run results and artifact fingerprints.

## Explicit boundaries / follow-up

1. Rollback can only undo writes supported by the backend's transaction implementation. Several
   backends implement `_rollback` as a no-op. Their immediately applied writes remain applied.
2. Multiple independent stores/connections do not commit atomically. A later commit failure cannot
   undo a store already committed. `withParent` is not a promise of JDBC savepoint nesting; default
   SQLite single-connection stores are not proof of concurrent transaction isolation.
3. Shared transactions retain idle-batched durability: successful calls can be uncommitted when a
   later failed call aborts the batch. Idle-expiry/acquisition races and shutdown under concurrent
   borrowers need dedicated stress tests; do not use this API as independent durable request units.
4. Callers must not continue writing after rollback, retain transactions beyond their scope, launch
   unjoined writers, or explicitly commit midway and expect later failure to undo that commit.
   Cancellation/interruption and process termination are not fully covered by these failure tests.
5. View maintenance remains post-commit, not atomic with source writes. Truncate notifications still
   use a store-wide flag rather than a transaction-scoped event; concurrent/aborted truncation needs
   separate correction and acceptance tests. Source/derived-table crash recovery remains necessary.
6. Connection loss during commit has an ambiguous outcome. Use idempotency and reconciliation rather
   than interpreting every exception as proof of rollback. Observer/close failures after a successful
   commit likewise cannot undo the committed transaction.
7. Native backend rollback, async search-update handlers, raw JDBC escape hatches and third-party
   binary compatibility need backend-specific acceptance before a general release claim.

The transaction patch and concurrent backend query fixes were tested in the same checkout. Preserve
both when preparing the final snapshot; this document is not evidence that every backend's runtime
behavior has been tested.
