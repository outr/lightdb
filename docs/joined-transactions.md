# Joined transactions and group commit

A transaction on one store can join a transaction on another, so that the owner's commit decides both: they commit
together or not at all.

```scala
db.events.transaction { owner =>
  owner.insert(event).next(
    db.states.transaction.joining(owner)(_.upsert(state))
  )
}
```

- `store.transaction.joining(owner)` creates a transaction that joins `owner` (or, when `owner` is itself joined, the
  transaction it joined). Release it before the owner commits; a typical place is a trigger's `transactionEnd` on the
  owner's store, which runs before the owner commits. Its `transactionCommitted` / `transactionRolledBack` run when the
  owner's outcome is known, before the owner's own.
- A failure in a joined transaction aborts the owner; the owner's release then fails with the cause.
- `store.supportsJoin` is the store manager's capability. A join the store cannot make is refused with
  `JoinRefusedException` (`store.transaction.canJoin(owner)` asks first).
- The op bound is the owner's (below); a joined transaction's writes count against it.

## Group commit and the op bound

A group-commit transaction holds at most a bound of ops, across it and every transaction joined to it. The bound is
declared when the transaction is created: `store.transaction.withGroupCommit(bound)`, or for every transaction of a
store, `db.store(Model).withGroupCommit(bound)()` (default 20,000; `withoutGroupCommit` opts one transaction out). The
write that crosses it fails with `TransactionBoundExceededException`, naming the store and the count; the transaction
aborts and writes nothing. It never flushes part of its writes to make room, so a large writer commits in explicit
chunks (`insert(stream, commitEvery = ...)`). A transaction created without the flag keeps today's implicit flush,
which is not atomic past the buffer size. `insert`/`upsert` of a stream without `commitEvery` on a group-commit
transaction counts every document.

## Backends

| Manager | Join | How |
|---|---|---|
| PostgreSQL | native | The joined transaction borrows the owner's connection (the same `ConnectionManager`, else refused). Its release flushes its batches without committing; its rollback discards them; its close leaves the connection open. The owner's `COMMIT` or rollback decides both. A joined state is never retried on a fresh connection: a failed read or flush on it aborts the owner instead. |
| Split over RocksDB | native | A store that opts into group commit buffers its writes; the owner's commit writes its ops and every joined transaction's in one `WriteBatch` across their column families (one WAL record), then updates the search index. Joins need a group-commit owner on the same RocksDB database. |
| Any other (SQLite, H2, LMDB, plain RocksDB, ...) | journal | Wrap the manager: `JournalStoreManager(manager)` or, for collections, `JournalCollectionManager(manager)`. |

### The journal wrapper

A journalled transaction and the ones joined to it buffer their writes. At commit the wrapper writes one commit-intent
record holding every op of the group to an intent store on the wrapped manager, in one atomic single-record write: that
is the commit point. A serial applier applies the intents to the wrapped stores in sequence order and deletes each only
after it (and so every earlier one) is applied; at open every intent still present is replayed in sequence order before
any read. Two commits writing one id therefore end with the later commit's value, live and after a crash.

- After the intent is durable the commit never fails. An apply error is retried in process with backoff
  (`JournalRetry`); if the retries run out the journal is marked failed: reads of its stores fail with
  `StoreFailedException` and new commits are refused until a reopen replays the intents.
- Ops are applied raw, with no store trigger firing: a derived write (a reverse edge, a joined row) is already in the
  intent and is applied once.
- The op bound of a group-commit transaction is the wrapped manager's declared record size (`recordBytes`, by default what the manager declares as `maxRecordBytes`) divided
  by the store's declared op size (`maxOpBytes`), never above 20,000; a write larger than the op size fails loud.
- The wrapped manager must provide an atomic, durable single-record write of its declared size, and idempotent put and
  delete by id. The wrapper gives atomic durability, not atomic visibility: while an intent is applied, another reader
  of the wrapped stores can see some of its rows before the rest.
- Reads by id see the transaction's buffer; scans, counts and queries read the wrapped stores' committed rows.
- Every journalled store must be registered with the database before it is initialized: replay at open applies an
  intent only to stores it knows.

## Behaviour changes on split collections over RocksDB

- **Rollback** (every split collection over RocksDB): a rolled-back write-through transaction puts storage back as it
  was (the rows it wrote are removed, the rows it overwrote or deleted are restored) and restores the index rows it
  touched from storage. Under `LuceneDurability.Immediate` a rollback no longer rolls back the shared index writer, which
  discarded every open transaction's pending index changes. A truncate is not undone.
- **Opted-in stores** (`withGroupCommit`): another transaction does not see a row until it commits; the transaction
  reads its own rows back by id; a query (or scan) sees neither its own buffered rows nor another's uncommitted ones.
  Stores that did not opt in keep writing through, with today's visibility.
- **An index error after the storage write** counts as committed: the commit succeeds, the ids are kept in
  `SplitCollection.indexPending` and re-indexed by id in process with backoff (`IndexRetry`), and the index is marked
  behind (Lucene's rebuild marker) as the crash backstop. A read by id finds the rows at once; a query finds them once
  the re-index lands, and `onIndexed` listeners hear of each id it lands. If the retries run out, one full re-index
  runs; if that fails, `indexFailure` is set and every query fails with `StoreFailedException` until a reopen rebuilds
  the index from storage.
- `reIndex` and `verify` write the search half directly, outside the group-commit bound.

## Readers that saw uncommitted rows

LightDB's own readers, checked against these changes:

- The split search half: index updates of opted-in stores move after the storage batch, so the index never holds a row
  storage lacks.
- Lucene's rebuild-from-storage marker: `beforeStorageChange` is called for every id of a group commit before the batch,
  and the marker is set on a post-batch index error.
- Prefix and range scans inside a transaction do not see its buffered rows; only reads by id do.
- Reverse-edge stores: the reverse transaction now joins the forward one when the stores can join, so both commit or
  roll back together (before, the reverse side committed at the forward side's `transactionEnd`, independently).
- `MaterializedBatchModel` processes its changes at `transactionEnd`, which also runs on the abort path; it writes in its
  own transactions, not the source's.
- `BasicStoreTrigger.upsert` reads the previous row by id, which sees the transaction's buffer.
- `IncrementingIdAllocator` probes the store in its own transaction at initialization only.
- View maintenance (`onCommittedChange`) runs after commit; on an opted-in split store a row whose index update failed
  is missing from a view query until the in-process re-index lands.
