# Licensed real-text release qualification

The new offline fixture is the unmodified UCI SMS Spam Collection (Almeida & Hidalgo, 2011), licensed CC BY 4.0 by the [UCI source page](https://archive.ics.uci.edu/dataset/228/sms+spam+collection). Attribution, archive/member hashes and explicit transformation are checked in under `fixtures/uci-sms/`.

The actual run consumed 5,574 labelled source rows (4,827 ham; 747 spam), yielding 5,171 exact-deduplicated samples. Splits use normalized-content SHA256 groups, so duplicate text cannot leak between train/eval/test. This is an infrastructure qualification, not a classification-quality evaluation. [Report](../evidence/hardening-sms-20261004/report.json).

Reversing transport row order produced the same release ID and shard hashes. A wrong source digest was rejected without changing CURRENT. A real subprocess died by SIGKILL after release-directory publication and before CURRENT; the old release stayed readable, retry recovered. A reader pin blocked expiration; unpin then expiration removed the old release without damaging CURRENT. All independent reconciliation invariants and the existing 33 regressions passed.

```bash
uv sync --frozen --extra test
uv run pytest -q
uv run python scripts/qualify_uci_sms.py --output evidence/new-sms-run
```

The command requires a fresh output directory and works without network after dependency installation. `consumer_release` in the report points to a real multi-shard release suitable for resumable-data-reader. The checked-in uv.lock now pins the complete environment; CI uses frozen sync and runs this actual process-crash drill. Raw artifacts and hashes are retained. Local POSIX process crashes do not establish power-loss or object-storage guarantees.
