"""Offline, licensed real-text publication, process-crash and retention drill."""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from corpus_release.release import canonical, digest, normalize, publish, verify
from corpus_release.lifecycle import expire, pin, unpin
from corpus_release.reconcile import reconcile


def qualify(out):
    out = out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    fixture = ROOT / "fixtures/uci-sms"
    metadata = json.loads((fixture / "provenance.json").read_text())
    archive = fixture / "sms-spam-collection.zip"
    assert digest(archive) == metadata["archive_sha256"]
    with zipfile.ZipFile(archive) as z:
        raw = z.read(metadata["member"])
    assert hashlib.sha256(raw).hexdigest() == metadata["member_sha256"]
    rows = []
    for index, line in enumerate(raw.decode("utf-8").splitlines(), 1):
        label, separator, text = line.partition("\t")
        assert separator and label in ("ham", "spam") and text
        group = hashlib.sha256(normalize(text, "nfc-lf-v2").encode()).hexdigest()
        bucket = int(group, 16) % 10
        rows.append({"id": f"uci-sms-{index:05d}", "text": text, "label": label,
                     "split": "test" if bucket == 0 else "eval" if bucket == 1 else "train"})
    assert len(rows) == metadata["expected_rows"]
    source, reverse = out / "source.jsonl", out / "reversed.jsonl"
    source.write_bytes(b"".join(canonical(row) + b"\n" for row in rows))
    reverse.write_bytes(b"".join(canonical(row) + b"\n" for row in reversed(rows)))
    catalog = out / "catalog"
    options = dict(license=metadata["license"], shard_rows=256, profile="nfc-lf-v2",
                   revision="uci-sms-v1", provenance=metadata)
    first = publish(source, catalog, expected_source_sha256=digest(source), **options)
    repeated = publish(reverse, out / "reordered-catalog", **options)
    assert first["dataset_id"] == repeated["dataset_id"]
    audit = reconcile(catalog)
    assert audit["passed"] and audit["source_lines"] == len(rows)
    before = (catalog / "CURRENT").read_bytes()
    try:
        publish(source, catalog, expected_source_sha256="0" * 64, **options)
    except ValueError as exc:
        assert "source changed" in str(exc)
    else:
        raise AssertionError("source digest mismatch accepted")
    assert (catalog / "CURRENT").read_bytes() == before
    # A separate real process dies after the immutable release is installed but
    # before CURRENT; the previous reader-visible release must remain intact.
    command = [sys.executable, "-m", "corpus_release.cli", "publish", str(source),
               str(catalog), "--license", metadata["license"], "--profile", "nfc-lf-v2",
               "--shard-rows", "256", "--revision", "uci-sms-recovery-v2"]
    env = dict(os.environ, PYTHONPATH=str(ROOT / "src"), CORPUS_KILL_AT="before_pointer")
    killed = subprocess.run(command, env=env, capture_output=True, text=True, timeout=60)
    assert killed.returncode == -signal.SIGKILL
    assert (catalog / "CURRENT").read_bytes() == before
    assert verify(catalog)["dataset_id"] == first["dataset_id"]
    env.pop("CORPUS_KILL_AT")
    recovered = subprocess.run(command, env=env, capture_output=True, text=True, timeout=60)
    assert recovered.returncode == 0, recovered.stderr
    current = json.loads(recovered.stdout)
    assert current["dataset_id"] != first["dataset_id"] and reconcile(catalog)["passed"]
    pin(catalog, first["dataset_id"], "qualification-reader")
    try:
        expire(catalog, first["dataset_id"])
    except ValueError as exc:
        assert "pinned" in str(exc)
    else:
        raise AssertionError("pinned release expired")
    unpin(catalog, first["dataset_id"], "qualification-reader")
    expiration = expire(catalog, first["dataset_id"])
    assert expiration["expired"] and verify(catalog)["dataset_id"] == current["dataset_id"]
    report = {"passed": True, "dataset": metadata, "input_rows": len(rows),
              "label_counts": dict(Counter(row["label"] for row in rows)),
              "reconciliation": audit, "deterministic_reorder": True,
              "rejected_source_hash": True, "kill_exit_code": killed.returncode,
              "recovered": current, "pinned_expiration_rejected": True,
              "old_release_expiration": expiration,
              "consumer_release": str(catalog / "releases" / current["dataset_id"]),
              "scope": "Real licensed text; two exact publications and actual local process SIGKILL. No model quality or power-loss guarantee.",
              "commands": [{"argv": command, "returncode": killed.returncode,
                            "stdout": killed.stdout, "stderr": killed.stderr},
                           {"argv": command, "returncode": recovered.returncode,
                            "stdout": recovered.stdout, "stderr": recovered.stderr}],
              "code_sha256": {str(p.relative_to(ROOT)): digest(p)
                              for p in sorted((ROOT / "src").rglob("*.py"))},
              "script_sha256": digest(Path(__file__)), "uv_lock_sha256": digest(ROOT / "uv.lock")}
    (out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed": True, "records": audit["records"],
                      "report": str(out / "report.json")}))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    qualify(parser.parse_args().output)
