# lightdb-indexeddb (spike)

A browser-persisted LightDB store on IndexedDB, built on the Scala.js cross-build of `lightdb-core`.
This module and `coreJS` are deliberately **not** in the root aggregate, so `sbt test` and CI are unaffected.

## Running the tests

```sh
cd indexeddb && npm install      # once: installs the fake-indexeddb shim for Node
sbt indexeddb/test
```

The tests run in Node against `fake-indexeddb`, an in-memory IndexedDB implementation, not in a real browser.

## What works

- `lightdb-core` compiles and links for Scala.js. The platform differences are isolated in `LightDBPlatform`
  (core/.jvm and core/.js) plus a JS `Spatial` that computes distances but rejects shape relations (JTS is JVM-only).
- Key-value operations: insert, upsert, get by id, exists, delete, count, stream, truncate.
- Commits are atomic: writes are buffered in the LightDB transaction and applied in one IndexedDB transaction.
  A transaction reads its own writes, and rollback discards the buffer, so it is exact.
- Data persists across dispose and reopen. `StoredValue.modify` stays serialized across async boundaries.

## Why writes are buffered

IndexedDB commits a transaction automatically as soon as it has no pending requests and control returns to the
event loop. A LightDB transaction runs arbitrary asynchronous work between writes, so it can never hold an IndexedDB
transaction open. Every request of one IndexedDB transaction is therefore issued synchronously at commit (see `IDB`).

## Known limitations

1. **rapid's stream terminal operations block.** `Stream.toList`, `toVector`, `count` and `drain` evaluate pulls
   through `Task.quickEval`, which falls back to a blocking await. On Scala.js that throws for any stream that
   touches IndexedDB. `fold` is fully task-based and works. This affects user code and core internals alike
   (for example `Transaction.insert(Seq)`), and needs fixing in rapid, not here.
2. **No filtered queries.** Like `HashMapStore`, this store supports id lookups only. The `traversal` module compiles
   for Scala.js unchanged and is the natural query layer, but needs a prefix-scanning backing store (feasible with
   IndexedDB key ranges), limitation 1 fixed, and its graph traversal rewritten without `.sync()`.
3. Streams and counts load the whole object store (`getAll`); a cursor-based implementation would scale better.
4. No spatial relations in the browser, and no shutdown hook: writes are durable at each commit instead.
