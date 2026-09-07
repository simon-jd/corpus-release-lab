# Corpus Release Lab

A LakeBridge extension for traceable training-data releases: read a validated Iceberg snapshot, preserve its provenance, deterministically clean and deduplicate text in DuckDB, and publish immutable Parquet shards through a crash-safe local catalog.

The standalone JSONL publisher also works without LakeBridge. The project qualifies publication and consumption contracts; it does not claim large-scale crawling, semantic leakage elimination, or model-training performance.

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[test]'
.venv/bin/pytest -q
.venv/bin/corpus-release publish input.jsonl catalog --license CC0-1.0
.venv/bin/corpus-release verify catalog
.venv/bin/corpus-release reconcile catalog
PYTHONPATH=src .venv/bin/python scripts/benchmark.py --work .run/scale
PYTHONPATH=src .venv/bin/python scripts/evaluate_near_duplicates.py --from-fixture
```

Input JSONL contains `text`, optional `id`, and explicit `split` (default `train`). `tests/test_release.py` contains inspectable synthetic examples. The license argument declares caller-supplied rights; it does not independently verify them.

| Evidence | Observed result |
|---|---|
| 1 GiB synthetic source | 255,318 records; repeated publication produces the same content-addressed dataset ID |
| Actual mid-shard process death | SIGKILL while a 6 MiB Parquet file had no valid footer; old CURRENT remained readable; retry matched the expected full release |
| Publication boundaries | Seven kill points, with both fresh and already-published catalogs |
| Retention | Protected pins/CURRENT; interrupted expiration resumes from a durable tombstone |
| Independent reconciliation | Manifest counts, source-line coverage, audit/sample bijection, text hashes and exact cross-split leakage all checked |
| Published human labels | 400 PAWS pairs; false positives and low recall documented, so lexical near-duplicates remain review-only |
| Real LakeBridge integration | Spark/Iceberg published snapshot exports; old snapshot stable after a new release; 4 added / 5 removed / 95 unchanged corpus texts |

Start with the [data card](docs/DATA_CARD.md), [transformation and publication contract](docs/CONTRACT.md), [recovery evidence](docs/RECOVERY.md), [near-duplicate review](docs/NEAR_DUPLICATES.md), and [LakeBridge integration](docs/LAKEBRIDGE_EXTENSION.md). [Qualification results](docs/QUALIFICATION.md) connect measured claims to commands and raw artifacts.

The benchmark generates at least 1 GiB of deterministic project-owned text and measures a publication subprocess. Raw scale bytes remain under `.run/` and can be regenerated; JSON reports retain hashes, source-code fingerprints, time/RSS and per-stage Linux I/O counters. Failed attempts remain separate evidence. Measurements do not enter dataset identity. Synthetic hash-word throughput is not representative of natural-corpus quality or tokenization.

Retention commands include `pin`, `unpin`, `expire`, `diff`, and `cleanup-staging` (dry run by default). `publish --revision <name>` expresses intentional reprocessing. Expired IDs cannot be silently resurrected. The catalog contract is local POSIX with cooperating writers; no distributed HA or physical power-loss guarantee is claimed.

Implementation references: [DuckDB Parquet row groups](https://duckdb.org/docs/stable/data/parquet/tips), [PAWS source and human annotation](https://github.com/google-research-datasets/paws). Code is MIT; the PAWS data retains its separate upstream permission text.
