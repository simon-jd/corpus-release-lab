# Corpus Release Lab

A local training-data publication contract with immutable Parquet releases, per-line provenance, evaluation-priority exact deduplication, content-addressed manifests and crash-safe publication.

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[test]'
.venv/bin/pytest -q
corpus-release publish input.jsonl catalog --license CC0-1.0
corpus-release verify catalog
.venv/bin/python scripts/benchmark.py --work .run/scale
```

Use `.venv/bin/corpus-release` if the environment is not activated. Input examples are in `tests/test_release.py`. The benchmark generates at least 1 GiB of unique synthetic text and runs publication in a separate measured process. Raw data stays under `.run/` and is not committed. Small JSON evidence records failures as well as successes.

Read [the exact contract and limitations](docs/CONTRACT.md). This is ongoing implementation, not a completed portfolio claim. Pending: reviewed near-duplicate labels, stage-by-stage byte accounting, LakeBridge integration, and broader qualification. The local catalog does not modify Iceberg metadata.

Implementation references: [DuckDB Parquet row groups](https://duckdb.org/docs/stable/data/parquet/tips), [Arrow pandas integration](https://arrow.apache.org/docs/python/pandas.html). Dependency versions are pinned in `pyproject.toml`.
