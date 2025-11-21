# Vector API Usage (Java)

This document summarizes how the Datalevin Java bindings interact with the
native USearch–LMDB integration layer, covering the lifecycle of vector
domains, staging mutations, and coordinating multi-process recovery.

## Prerequisites

- The native `dtlv_usearch` shared library must be built and available on the
  library path. Running `./script/build` (Linux) or `./script/build-macos`
  produces the requisite artifacts.
- Java clients link against the generated `DTLV` JavaCPP bindings under
  `src/java/datalevin/dtlvnative/DTLV.java`.
- An LMDB environment must exist (created via `DTLV.mdb_env_create`,
  `mdb_env_open`, etc.) and configured with `mdb_env_set_maxdbs` large enough
  to hold the per-domain `usearch-*` DBIs.

## Creating and Initializing a Vector Domain

```java
DTLV.MDB_env env = new DTLV.MDB_env();
expect(DTLV.mdb_env_create(env) == 0, "Failed to create env");
expect(DTLV.mdb_env_set_maxdbs(env, 64) == 0, "Failed to set max DBs");
expect(DTLV.mdb_env_open(env, envPath, DTLV.MDB_NOLOCK, 0664) == 0,
        "Failed to open env");

DTLV.dtlv_usearch_domain domain = new DTLV.dtlv_usearch_domain();
expect(DTLV.dtlv_usearch_domain_open(env, "vectors", fsRoot, domain) == 0,
        "Failed to open domain");

DTLV.usearch_init_options_t opts = createOpts(dimensions);
DTLV.MDB_txn txn = new DTLV.MDB_txn();
expect(DTLV.mdb_txn_begin(env, null, 0, txn) == 0, "Failed to begin txn");
expect(DTLV.dtlv_usearch_store_init_options(domain, txn, opts) == 0,
        "Failed to store init opts");
expect(DTLV.mdb_txn_commit(txn) == 0, "Failed to commit init opts");
```

- `domain` binds a logical vector space (`vectors/usearch-*` DBIs) to a
  filesystem root used for WAL directories and reader pins.
- `createOpts` should populate metric, scalar kind, dimensions, and other
  USearch parameters. The helper in `Test.java` demonstrates typical defaults.

## Activating Handles and Running Queries

```java
DTLV.dtlv_usearch_handle handle = new DTLV.dtlv_usearch_handle();
expect(DTLV.dtlv_usearch_activate(domain, handle) == 0, "activate failed");

DTLV.usearch_index_t index = DTLV.dtlv_usearch_handle_index(handle);
PointerPointer<BytePointer> error = new PointerPointer<>(1);
error.put(0, (BytePointer) null);
long found = DTLV.usearch_search(index,
        queryVector,
        DTLV.usearch_scalar_f32_k,
        k,
        keysPointer,
        distancesPointer,
        error);
expectNoError(error, "search failed");
```

- Every thread/process needs to call `dtlv_usearch_activate` before issuing
  USearch queries; this streams the latest LMDB snapshot and delta tail into an
  in-memory handle.
- After running queries, call `DTLV.dtlv_usearch_deactivate(handle)` to release
  the USearch index when the process shuts down.
- Use `DTLV.dtlv_usearch_refresh(handle, txn)` (with an LMDB read transaction)
  to catch up to newer `log_seq` values if a process lags behind.

## Staging Vector Updates

```java
DTLV.MDB_txn txn = new DTLV.MDB_txn();
expect(DTLV.mdb_txn_begin(env, null, 0, txn) == 0, "begin write txn failed");

DTLV.dtlv_usearch_update update = new DTLV.dtlv_usearch_update();
update.op(DTLV.DTLV_USEARCH_OP_ADD);
update.key(keyPointer);
update.key_len(Long.BYTES);
update.payload(payloadPointer);
update.payload_len(dimensions * Float.BYTES);
update.scalar_kind((byte) DTLV.usearch_scalar_f32_k);
update.dimensions((short) dimensions);

DTLV.dtlv_usearch_txn_ctx ctx = new DTLV.dtlv_usearch_txn_ctx();
expect(DTLV.dtlv_usearch_stage_update(domain, txn, update, ctx) == 0,
        "stage update failed");
expect(DTLV.dtlv_usearch_apply_pending(ctx) == 0,
        "apply pending failed");
expect(DTLV.mdb_txn_commit(txn) == 0, "LMDB commit failed");
expect(DTLV.dtlv_usearch_publish_log(ctx, 1) == 0,
        "publish log failed");
DTLV.dtlv_usearch_txn_ctx_close(ctx);
```

- `stage_update` buffers the WAL payload and writes the serialized delta (key +
  raw vector bytes) into the `usearch-delta` DBI inside the caller’s
  transaction.
- `apply_pending` seals the WAL, writes delta rows, and updates metadata before
  the LMDB commit.
- `publish_log` replays the sealed WAL into *all* in-process USearch handles.
  If a process crashes before publish, the WAL stays on disk and will be
  replayed during the next `dtlv_usearch_activate` or explicit
  `dtlv_usearch_publish_log`.

## Checkpoints and Recovery

- `dtlv_usearch_checkpoint_write_snapshot(domain, index, snapshotSeq, writerUuid, chunkCountOut)`
  serializes the current USearch index into chunked LMDB entries.
- `dtlv_usearch_checkpoint_finalize(domain, snapshotSeq, pruneLogSeq)`
  atomically updates metadata, prunes deltas ≤ `pruneLogSeq`, and removes the
  `checkpoint_pending` key.
- After a crash (or when adopting a stale environment), call
  `dtlv_usearch_checkpoint_recover(domain)` before activation. This inspects
  `checkpoint_pending`, `sealed_log_seq`, and on-disk WAL files to ensure any
  partial work is cleaned up before readers proceed.

## Multi-Process Usage

- Each JVM process must bring its own LMDB environment handle and open the same
  domain name. WAL files (`<root>/pending/*.ulog*`) coordinate cross-process
  updates; only one LMDB write transaction may run at a time due to LMDB’s
  single-writer semantics.
- The Java harness (`testUsearchJavaMultiProcessIntegration`) demonstrates how
  to spawn writer/reader JVMs, including crash-before-publish and
  checkpoint-crash scenarios. Use `ProcessBuilder` (or your task runner of
  choice) to sequence these roles in real deployments.

## Implementation Notes

- The multi-process helpers live in `src/java/datalevin/dtlvnative/Test.java`
  and are intended both as integration tests and as executable examples for
  Datalevin developers. The `MultiProcessWorker` class exposes `writer`,
  `reader`, `crash-writer`, and `checkpoint-crash` roles to make it easy to
  script various workflows.
- During staging, payloads are copied into both the WAL frame and the LMDB
  delta row, so crash recovery can always replay deltas even if a host dies
  after LMDB commit but before the in-memory handles catch up. Once a
  checkpoint finalizes, the retained delta tail becomes small (only the gap
  since the latest snapshot).
- Checkpoint crash handling relies on metadata entries (`checkpoint_pending`,
  per-chunk LMDB keys) to identify incomplete snapshots. Re-running
  `dtlv_usearch_checkpoint_recover` is idempotent; it removes torn chunks and
  clears `checkpoint_pending` so another process may restart the checkpoint.
- USearch is the canonical store of vector bytes. LMDB only keeps them inside
  the delta log until a checkpoint finalizes, at which point old deltas are
  pruned. Operators should therefore back up both the LMDB environment and the
  USearch snapshots/WAL files together.
- All file-system interactions (WAL directories and optional reader-pin state)
  sit under the
  `filesystem_root` you pass to `dtlv_usearch_domain_open`. Ensure the user
  running Datalevin has permission to read/write these paths, and include them
  in your backup/restore policies alongside the LMDB data file.

## Operational Playbooks

- Partial checkpoints: if `dtlv_usearch_checkpoint_write_snapshot` returns
  `MDB_MAP_FULL`, the `checkpoint_pending` marker remains set so you can
  increase the LMDB map size (`mdb_env_set_mapsize`), run
  `dtlv_usearch_checkpoint_recover(domain)` to clear the partial snapshot, and
  then retry the checkpoint/finalize sequence. Use `checkpoint_pending` as your
  signal that a previous attempt needs cleanup before scheduling another
  snapshot.
- Pinned readers: the pins LMDB lives under
  `<filesystem_root>/reader-pins/reader-pins.mdb`. If long-lived reads stall
  compaction/checkpoint planning, inspect that file (or call
  `dtlv_usearch_touch_pin/release_pin`) to see which `(snapshot_seq, log_seq)`
  is still pinned. Expired entries can be removed with
  `dtlv_usearch_release_pin` or by deleting the pins env while the process is
  quiesced; the domain recreates it on open, and readers can refresh handles to
  advance to the latest snapshot.
- Checksum fault injection: set `DTLV_FAULT_WAL_CRC=1` to corrupt outgoing WAL
  frame CRCs or `DTLV_FAULT_SNAPSHOT_CRC=1` to write bad snapshot chunk
  checksums. These toggles help exercise checksum failure paths without manual
  file mangling.
