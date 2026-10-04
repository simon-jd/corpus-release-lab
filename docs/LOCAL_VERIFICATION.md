# Local verification

These are local CPU checks for this repository refresh, separate from the owner-confirmed cloud result round in [experience-bank-results.json](experience-bank-results.json).

All commands below returned exit code 0. Exact command arrays, output, durations and source hashes are in [the verification record](local-verification.json).

| Command | Outcome | Seconds |
|---|---|---|
| `python -m pytest -q -p no:cacheprovider` | 33 passed in 7.13s | 7.36 |

Changes: Added the opt-in nfc-lf-v2 publication profile: NFC/LF normalization preserving case and whitespace, canonical source ordering, evaluation-first stable-ID deduplication, byte comparison within hash groups, stable output ordering and 4096-row groups. Legacy releases remain reproducible through legacy-v1. The canonical profile buffers source records while sorting and makes no constant-memory claim.

The repository keeps earlier artifacts unchanged unless the corresponding current verifier explicitly regenerates its own attestations. Historical timing records are not new local measurements.
