# Publication contract v1

The main executor is DuckDB; Python streams input and Arrow writes bounded preparation batches. Input is UTF-8 JSONL with `text`, optional `id`, and explicit `split` (`train`, `validation`, `test`; default train). The source is caller-owned or explicitly licensed: supplying a license declares permission, it does not independently verify rights.

Normalization uses Unicode NFKC and whitespace collapse, preserving case. Empty, replacement-character, malformed/invalid UTF-8 and fewer-than-20-character records are rejected. Every source line appears in `audit.parquet`; original bytes remain in `source.jsonl`. The audit contains source line, ID, normalized hash, split, normalization flag and decision. It deliberately avoids duplicating the full source text.

Exact deduplication is by normalized UTF-8 SHA-256. Test takes priority over validation, which takes priority over train; ties use source line. This prevents identical normalized text appearing across splits. It does **not** detect semantic leakage. Shards are globally ordered by hash. Explicit splits are preserved; seed is recorded for downstream reproducibility and does not randomize v1 splits. Tokenization and near-dedup filtering are disabled.

Dataset identity hashes canonical manifest content including source hash, rules, license, dependency versions, shard hashes and audit hash. Byte determinism is promised within the pinned implementation/version environment, not across arbitrary DuckDB/Arrow releases. Runtime measurements are external evidence and excluded from identity.

## Local catalog and recovery

This is a standalone POSIX local catalog, not an Iceberg catalog or modification to LakeBridge metadata. All cooperating writers hold `.catalog.lock` using `flock`. Each writes a private staging directory, fsyncs artifacts and manifest, renames to `releases/<id>`, fsyncs the releases directory, then atomically replaces and fsyncs `CURRENT`. The pointer replacement is the publication linearization point. Readers discover only CURRENT or an explicit retained identity and validate the manifest and all hashes. Full verification is intentionally expensive.

A crash before pointer replacement leaves the prior CURRENT usable (or no publication on a fresh catalog). A crash after replacement exposes a fully persisted release. Unreferenced complete versions and partial stages are retained for diagnosis. A retry reconstructs deterministically and verifies any existing identical release. SIGKILL tests are process-crash tests on the local filesystem; they do not prove physical power-loss behavior, NFS semantics or multi-host HA.

## Retention, deletion and reprocessing

No automatic expiration. Old versions remain readable by ID. `lifecycle.pin(root,id,owner)` blocks deletion for an active consumer; owners must unpin when finished. Consumers requiring retention must pin before use; an unpinned read can race explicit expiration. CURRENT and pinned releases cannot expire. Explicit expiration leaves a durable manifest tombstone before deleting data; expired identities cannot be republished accidentally. Reprocessing needs a distinct recorded configuration/source identity. Process-death recovery of partially deleted directories is not yet automated. Staging cleanup is a separate operator maintenance task and must hold the catalog lock.

The prototype keeps source copies per release; storage amplification is intentional for provenance and not yet optimized. It is not merged with LakeBridge. An adapter and integration evidence remain required before describing this as an existing LakeBridge feature.
