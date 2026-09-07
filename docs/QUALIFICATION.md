# Qualification results

This qualification closes the baseline A contracts with actual local data and process failures. It supports the measured claims below, not production-scale training quality, GPU throughput or distributed recovery.

## Full-input run

`evidence/scale-qualified.json` was produced by the final implementation source hashes recorded in `evidence/qualified-source-fingerprint.json`. Input is 1,073,743,632 bytes and 255,318 unique synthetic records. Publication took **12.825 seconds**, or **19907.4 accepted records/second**, with **1,495,712 KiB peak process RSS**. This was a shared-host, cached-source run that rebuilt and verified an already retained version; it is not a cold-storage throughput claim. Initial/new-catalog runs are retained separately.

Dataset ID is `9e5237536671de720c75eba80cb77decd052bae09b89665ecb409e4f227faa7e`. It matches the pre-instrumentation and repeated instrumented runs. None of the timings/I/O measurements enter the immutable manifest.

| Stage | Seconds | Syscall read bytes | Syscall write bytes | Storage read bytes | Storage write bytes |
|---|---:|---:|---:|---:|---:|
| source_snapshot | 0.698 | 2147487392 | 1073743632 | 0 | 1073745920 |
| normalization | 3.591 | 1083984055 | 554741987 | 0 | 554749952 |
| duckdb_load | 1.764 | 847943381 | 1374141486 | 4096 | 1239203840 |
| exact_deduplication | 0.215 | 4718724 | 10753051 | 0 | 10760192 |
| audit_export | 0.276 | 132 | 20517531 | 0 | 20520960 |
| shard_assignment | 0.179 | 132 | 10490285 | 0 | 10493952 |
| shard_export | 3.445 | 3601236100 | 1255991577 | 0 | 1201565696 |
| database_flush_close | 0.801 | 406323336 | 565973045 | 0 | 565977088 |
| temporary_artifact_cleanup | 0.080 | 136 | 0 | 0 | 0 |
| artifact_hashing | 0.763 | 1657203565 | 0 | 0 | 0 |
| publication | 0.991 | 1657204634 | 1134 | 0 | 8192 |

Counters are Linux process observations, not physical disk telemetry or row payload sizes. Syscall reads include cache hits and observer overhead; storage reads may be zero. Writeback cancellation and temporary-file cleanup are reported separately in raw JSON. The process-lifetime RSS high-water mark cannot be added across stages. Metrics do not cover time waiting to acquire the catalog lock.

## Verification scope

- **31 local tests** cover deterministic output, rejected/normalized source traceability, exact cross-split deduplication, manifest corruption, concurrent writers, seven SIGKILL boundaries for both initial and existing catalogs, interrupted expiration, pin protection, staging cleanup, explicit reprocessing, CLI behavior, upstream receipt corruption and a source-change race between validation and copy.
- `scale-reconciliation.json` independently scans full Parquet artifacts and raw line count: nine semantic checks passed for all 255,318 samples. It is stronger than a hash-only verifier and includes a regression where checksummed artifacts contain inconsistent text/sample IDs. The two-thread verifier used 222,416 KiB peak RSS in the retained resource measurement.
- `scale-interrupted.json` records actual SIGKILL while an incomplete 6 MiB Parquet shard existed. The prior release remained readable and retry produced the expected full dataset ID. Raw subprocess stdout/stderr hashes and files are included.
- `near-duplicates.json` evaluates 400 unchanged published human labels. It retains confusion matrices, per-pair predictions and false positives. The evaluation is reproducible from the committed fixture without network access, and CI checks exact report equality. Near-duplicates remain review-only because the lexical rules are not adequately reliable.
- `lakebridge-integration.json` plus the indexed raw bundle records newly run Spark/Iceberg releases, stable old-snapshot exports, provenance-bound corpus IDs, both reconciliations and the 4-added/5-removed/95-unchanged version diff. `lakebridge-final-publish.json` also exercises the final receipt-copy integrity guard on that real export.

## Independent boundaries and remaining limits

The PAWS stress test is an adversarial semantic-equivalence proxy, not corpus-specific duplicate prevalence or a new annotation campaign. The LakeBridge source is a synthetic typed-event JSON projection, not customer data. Local fsync/flock/process-crash experiments do not prove physical power-loss behavior, NFS semantics, distributed fencing or HA. Tokenization and model training remain intentionally outside scope. The source snapshot-copy check binds receipts to copied bytes; hashes do not authenticate an untrusted exporter.
