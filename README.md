# Corpus Release Lab — Auditable Training Data Releases

LakeBridge corpus-publication extension (Python 3.14, DuckDB 1.5.5, PyArrow 25.0.1, Parquet, local ext4, 4 CPU / 8 GiB) that deduplicates 266,000 simulated source records into 263,000 audit-decided outputs with evaluation-priority exact dedup, publishes a source/snapshot/schema-bound immutable manifest through same-disk staging plus fsync and an atomic current pointer, and honors reader release pins through lock-checked expiry tombstones. Fifteen crash-window cases and 10 pin-versus-cleaner races keep the pointer and files consistent; crash verification covers process exit and SIGKILL only.

## Latest reported results

These results are the owner-confirmed test round from a separate cloud environment, recorded in the experience bank. Hardware, dataset, fault-injection, timing and denominator boundaries below belong to that round. This repository refresh runs local regression checks separately; it does not relabel them as a rerun of the reported experiment.

1. Extended LakeBridge with deterministic corpus releases: fixed the source snapshot_id and export receipt, wrote schema, preprocessing rules, transform version and output file hashes into an immutable manifest, and ran a simulated round over 266,000 source records (train 248,000 + eval 18,000 with 3,000 exact cross-split overlaps) producing 263,000 audit decisions (train 245,000 + eval 18,000, with 3,000 train duplicates deleted, sum equal to input) on Python 3.14 / DuckDB 1.5.5 / PyArrow 25.0.1.
2. Made dedup reproducible: UTF-8 NFC and LF normalization preserving case, SHA256 grouping followed by bytewise comparison of normalized content (the hash only narrows candidates), evaluation side wins cross-split duplicates, within-split duplicates keep the smallest record_id, and output sorts stably by split and id.
3. Verified determinism by shuffling the same input 3 times: Parquet encoding and writer parameters fixed, the canonical manifest excluded run time and log paths, and each output file plus the canonical manifest produced identical SHA256 across 4 output files of 65,750 rows each with 4096-row row groups (boundaries also exposed to the reader).
4. Implemented crash-recoverable publication on one local filesystem: same-disk temporary directory, file and directory fsync, then atomic replacement of the current pointer; 5 fault windows x 3 runs = 15 fault cases left current on the old release for the first 9, exposed the complete new release for the next 3, and resumed tombstoned deletion of expired unpinned objects for the last 3, with no case where current pointed at a missing file.
5. Implemented pin-aware retention and tamper checks: 10 repeated reader-holding-R1 versus cleaner races showed that once a pin was acquired the R1 files were not deleted and once a tombstone committed new pins failed, so an expired release cannot be resurrected; 8 tampering cases that changed one bit of the receipt snapshot or file hash were all rejected, and the new round was validated with 30 tests split 10 dedup/normalization, 10 publication/recovery and 10 retention/receipt.

The complete reported result and limitation text is preserved in [the machine-readable result record](docs/experience-bank-results.json). Earlier checked-in measurements remain separate in [the previous README](README.previous.md) and the original evidence directories.

## Implementation and verification

Added the opt-in nfc-lf-v2 publication profile: NFC/LF normalization preserving case and whitespace, canonical source ordering, evaluation-first stable-ID deduplication, byte comparison within hash groups, stable output ordering and 4096-row groups. Legacy releases remain reproducible through legacy-v1. The canonical profile buffers source records while sorting and makes no constant-memory claim.

| Reported bullet | Implementation | Executable checks |
|---|---|---|
| 1–4 | [src/corpus_release/release.py](src/corpus_release/release.py) | [tests/test_canonical_profile.py](tests/test_canonical_profile.py) |
| 4, 5 | [src/corpus_release/lifecycle.py](src/corpus_release/lifecycle.py) | [tests/test_release.py](tests/test_release.py) |
| 5 | [src/corpus_release/lakebridge.py](src/corpus_release/lakebridge.py) | [tests/test_lakebridge_receipt.py](tests/test_lakebridge_receipt.py) |

These links identify the corresponding contracts, not an assertion that the current local suite reruns every cloud benchmark size, timing cell or hardware configuration. See [local verification](docs/LOCAL_VERIFICATION.md) for the exact commands and outcomes from this refresh.

## Reproduce the checks

Run from the repository root using the pinned Python/Go version in the project configuration.

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[test]'
.venv/bin/python -m pytest -q
.venv/bin/corpus-release publish input.jsonl catalog --license CC0-1.0 --profile nfc-lf-v2 --shard-rows 65750
```

Full experiment setup, service dependencies, archived artifacts and longer-running commands are retained in [the previous README](README.previous.md).

## Scope and interpretation

- 263,000 is the new round's retained output (from 266,000 raw inputs, 3,000 train duplicates deleted); the historical 255,318-record figure is a different round's input. Keep the rounds and labels separate, do not sum them, and do not attach old retained-run labels or any storage-size figure to the 263,000 count.
- Crash verification is limited to process exit and SIGKILL: fsync call ordering is not evidence about power loss or disk controller behavior, and power-loss behavior remains unverified.
- Exact dedup removes only identical content under the stated normalization; paraphrase, image rewrite and semantic overlap need separate evaluation, and near-duplicate assessment stays review-only and does not establish leakage prevention.
- The 15 fault cases are 5 windows x 3 runs, not 15 independent scenarios; the window outcomes (first 9 old release, next 3 new release, last 3 resumed deletion) belong to this simulated matrix.
- Local ext4 POSIX filesystem with 4 CPU / 8 GiB and synthetic inputs; this is a LakeBridge extension for data-preparation boundaries, not an independent production deployment, and it should not be duplicated as a second ETL entry on the same resume.
- The timing endpoint runs from reading the already-fixed receipt to persisting the current pointer; source-side export and downstream training are excluded.
- A matching SHA256 is not record identity: dedup still compares normalized bytes, the dedup rules are fixed in the manifest so a rule change does not silently reuse an identity, and callers still own pin lifetime.
