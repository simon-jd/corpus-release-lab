# Failure and recovery evidence

## Publication

The tests kill real Python processes using SIGKILL at seven boundaries: before shards, after a shard appears, after all shards, before manifest, after manifest, before CURRENT replacement, and after CURRENT replacement. Both a pre-existing CURRENT and a fresh catalog are covered. Before pointer replacement a fresh reader finds no release; an existing reader resolves its prior complete version. After replacement the new version verifies. Retrying converges on the expected immutable identity.

`evidence/scale-interrupted.json` supplements fixture boundaries with a large live-writer experiment. The observer waits for an on-disk Parquet shard whose footer cannot yet be parsed, then kills the actual publishing process. The prior CURRENT remains readable, and a full retry must reproduce the known 1 GiB dataset identity. The observer is bounded by a timeout and fails the experiment if it does not see an incomplete shard.

Concurrency uses the local catalog's flock contract; separate writer processes publish distinct retained snapshots and CURRENT points to one complete version. This does not qualify object storage, network filesystems, distributed fencing or machine power loss.

## Expiration and maintenance

Expiration first rejects CURRENT and pinned releases. A durable content-bound manifest tombstone is the visibility boundary; readers reject that identity immediately. Deletion can then be retried without relying on files already removed. Tests kill after tombstone creation, during file deletion and after deletion. Each retry completes, preserves CURRENT and remains idempotent.

An expired identity cannot be silently republished. Supply an explicit `--revision` for intentional reprocessing; the changed configuration creates a different identity. Existing snapshots have no automatic TTL. `cleanup-staging` is a separate age-filtered maintenance action, defaults to a dry run, and holds the writer lock. `--apply` removes eligible abandoned stages; it never deletes retained releases.

## Resource failures retained

The first 1 GiB implementation ran out of its 1 GB DuckDB allocation limit during audit export. A second attempt still retained too much text during ranking. The final pipeline ranks narrow metadata tables and joins text only when exporting shards, with 2,048-row Parquet row groups. Both initial failures are retained as `scale-initial-oom.json` and `scale-second-oom.json`.

An initial full reconciliation attempt also exhausted the same limit when DuckDB selected the host's default high thread count. The verifier now sets two threads and scans Parquet lazily; its successful full-input run used 222,416 KiB peak RSS. The failure was observed in tool session 96598; only this narrative, not a complete standalone stderr artifact, was retained for that first verifier attempt. The successful command/resource outputs are preserved independently.

Per-stage `/proc/self/io` deltas distinguish syscall bytes from storage-layer bytes. Cached reads can have zero storage-layer bytes. `cancelled_write_bytes` captures writes to temporary files subsequently cancelled/deleted; it must not be treated as completed durable output. Counters include library I/O and small observation overhead. RSS is a process high-water mark, not additive per-stage allocation. Timing experiments were run on a shared host and do not isolate collector overhead or claim stable production throughput.
