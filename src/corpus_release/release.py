"""POSIX local catalog. All cooperating writers serialize via flock.

Publication linearizes at atomic CURRENT replacement. Directory durability uses
fsync; remote/object filesystems are deliberately outside this contract.
"""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import tempfile
import time
import unicodedata

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from .metrics import Metrics


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def syncdir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic(path, data):
    path = Path(path)
    fd, tmp = tempfile.mkstemp(prefix=".write-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        syncdir(path.parent)
    finally:
        Path(tmp).unlink(missing_ok=True)


@contextlib.contextmanager
def lock(root):
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".catalog.lock").open("a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield


def failpoint(name):
    if os.environ.get("CORPUS_KILL_AT") == name:
        os.kill(os.getpid(), signal.SIGKILL)


def normalize(text, profile="legacy-v1"):
    if profile == "nfc-lf-v2":
        return unicodedata.normalize("NFC", text.replace("\r\n", "\n").replace("\r", "\n"))
    return " ".join(unicodedata.normalize("NFKC", text).split())


def _prepare(source, target, profile="legacy-v1"):
    """Keep original bytes; malformed records have a source line and reason."""
    schema = pa.schema([(n, t) for n, t in [
        ("line", pa.int64()), ("record_id", pa.string()), ("text", pa.string()),
        ("content_hash", pa.string()), ("requested_split", pa.string()),
        ("reason", pa.string()), ("changed", pa.bool_()),
    ]])
    batch = []
    with pq.ParquetWriter(target, schema, compression="zstd") as writer:
        with open(source, "rb") as f:
            for line, raw in enumerate(f, 1):
                rid, text, split, reason, changed = str(line), "", "train", "", False
                try:
                    obj = json.loads(raw.decode("utf-8", errors="strict"))
                    if not isinstance(obj, dict) or not isinstance(obj.get("text"), str):
                        raise ValueError("invalid_record")
                    rid = str(obj.get("id", line))
                    split = obj.get("split", "train")
                    if split not in ("train", "eval", "validation", "test"):
                        raise ValueError("invalid_split")
                    text = normalize(obj["text"], profile)
                    text.encode("utf-8", errors="strict")
                    changed = text != obj["text"]
                    if not text:
                        reason = "empty"
                    elif "\ufffd" in text:
                        reason = "replacement_character"
                    elif profile == "legacy-v1" and len(text) < 20:
                        reason = "too_short"
                except UnicodeError:
                    reason = "invalid_encoding"
                except (ValueError, TypeError):
                    reason = "invalid_record"
                batch.append(dict(line=line, record_id=rid, text=text if not reason else "",
                                  content_hash=hashlib.sha256(text.encode('utf-8', errors='replace')).hexdigest(),
                                  requested_split=split if isinstance(split, str) else "invalid",
                                  reason=reason, changed=changed))
                if len(batch) == 4096:
                    writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                    batch = []
        if batch:
            writer.write_table(pa.Table.from_pylist(batch, schema=schema))


def publish(source, root, *, license, seed=42, shard_rows=100_000, revision="v1", provenance=None, expected_source_sha256=None, profile="legacy-v1"):
    if profile not in {"legacy-v1", "nfc-lf-v2"}:
        raise ValueError("unsupported normalization profile")
    if not isinstance(revision, str) or not revision.strip():
        raise ValueError("nonempty processing revision required")
    if provenance is not None and not isinstance(provenance, dict):
        raise ValueError("provenance must be an object")
    if not license or type(shard_rows) is not int or shard_rows < 1:
        raise ValueError("license and positive shard_rows required")
    root, source = Path(root), Path(source)
    started = time.monotonic()
    metrics = Metrics()
    with lock(root):
        releases = root / "releases"
        releases.mkdir(exist_ok=True)
        stage = Path(tempfile.mkdtemp(prefix=".staging-", dir=root))
        # Partial stages are retained on failure for diagnosis; never discoverable by readers.
        with metrics.stage("source_snapshot") as measurement:
            shutil.copyfile(source, stage / "source.jsonl")
            source_hash = digest(stage / "source.jsonl")
            if expected_source_sha256 is not None and source_hash != expected_source_sha256:
                raise ValueError("source changed between receipt verification and snapshot copy")
            if profile == "nfc-lf-v2":
                # This profile's identity is the canonical record set, independent
                # of transport row order. Receipt provenance stays separately bound.
                records = [json.loads(line) for line in (stage / "source.jsonl").read_bytes().splitlines()]
                if any(not isinstance(row, dict) or not isinstance(row.get("id"), str)
                       or not row["id"] or not isinstance(row.get("text"), str) for row in records):
                    raise ValueError("canonical profile requires explicit string IDs and text")
                if len({row["id"] for row in records}) != len(records):
                    raise ValueError("canonical profile requires unique record IDs")
                records.sort(key=lambda row: (row.get("split", "train"), row["id"], canonical(row)))
                (stage / "source.jsonl").write_bytes(b"".join(canonical(row) + b"\n" for row in records))
                source_hash = digest(stage / "source.jsonl")
            measurement.update(input_artifact_bytes=source.stat().st_size, output_artifact_bytes=(stage / "source.jsonl").stat().st_size)
        config = {"contract": 1, "rules": "nfkc-whitespace-min20-v1", "seed": seed,
                  "tokenizer": None, "shard_rows": shard_rows, "license": license,
                  "split_policy": "explicit-evaluation-priority-v1",
                  "duckdb": duckdb.__version__, "pyarrow": pa.__version__}
        if profile == "nfc-lf-v2":
            config.update(rules="nfc-lf-case-preserving-v2", source_order="split-id-canonical-json",
                          row_group_size=4096, split_policy="evaluation-priority-min-record-id-v2")
        if revision != "v1":
            config["processing_revision"] = revision
        if provenance is not None:
            # Metadata binds upstream release/transform identity; text bytes are hashed separately.
            config["source_provenance"] = json.loads(canonical(provenance))
        with metrics.stage("normalization") as measurement:
            _prepare(stage / "source.jsonl", stage / "prepared.parquet", profile)
            measurement.update(input_artifact_bytes=(stage / "source.jsonl").stat().st_size, output_artifact_bytes=(stage / "prepared.parquet").stat().st_size)
        con = duckdb.connect(str(stage / "working.duckdb"))
        try:
            con.execute("SET threads=2")
            con.execute("SET enable_progress_bar=false")
            con.execute("SET preserve_insertion_order=false")
            con.execute("SET memory_limit='1GB'")
            con.execute("SET temp_directory=?", [str(stage / "spill")])
            with metrics.stage("duckdb_load"):
                con.execute("CREATE TABLE raw AS SELECT * FROM read_parquet(?)", [str(stage / "prepared.parquet")])
            # Evaluation wins exact duplicate groups; train never contains evaluation text.
            with metrics.stage("exact_deduplication"):
                if con.sql("SELECT count(*) FROM (SELECT content_hash FROM raw WHERE reason='' GROUP BY content_hash HAVING count(DISTINCT text)>1)").fetchone()[0]:
                    raise ValueError("content hash collision: normalized bytes differ")
                tie_break = "record_id, line" if profile == "nfc-lf-v2" else "line"
                con.execute(f"""CREATE TABLE ranked AS SELECT * EXCLUDE(text), row_number() OVER (
                    PARTITION BY content_hash, text ORDER BY CASE requested_split
                    WHEN 'test' THEN 0 WHEN 'eval' THEN 1 WHEN 'validation' THEN 1 ELSE 2 END, {tie_break}) AS rank
                    FROM raw WHERE reason=''""")
            with metrics.stage("audit_export"):
                con.execute("""CREATE TABLE audit AS SELECT r.* EXCLUDE(text),
                    CASE WHEN r.reason!='' THEN r.reason WHEN k.rank>1 THEN 'exact_duplicate'
                    ELSE 'accepted' END AS decision
                    FROM raw r LEFT JOIN ranked k USING(line)""")
                con.sql("SELECT * FROM audit ORDER BY line").write_parquet(str(stage / "audit.parquet"), compression="zstd", row_group_size=2048)
            with metrics.stage("shard_assignment"):
                ordering = "k.requested_split,k.record_id,k.content_hash" if profile == "nfc-lf-v2" else "k.content_hash"
                con.execute(f"""CREATE TABLE accepted AS SELECT k.content_hash AS sample_id,
                    k.requested_split AS split, k.line AS source_line, k.record_id,
                    row_number() OVER (ORDER BY {ordering}) AS ordinal
                    FROM ranked k WHERE k.rank=1""")
                count = con.sql("SELECT count(*) FROM accepted").fetchone()[0]
            failpoint("before_shards")
            with metrics.stage("shard_export"):
                for start in range(0, count, shard_rows):
                    path = stage / f"shard-{start // shard_rows:05d}.parquet"
                    con.sql(f"SELECT a.* EXCLUDE(ordinal), r.text FROM (SELECT * FROM accepted WHERE ordinal>{start} AND ordinal<={start+shard_rows}) a JOIN raw r ON a.source_line=r.line ORDER BY a.ordinal").write_parquet(str(path), compression="zstd", row_group_size=4096 if profile == "nfc-lf-v2" else 2048)
                    failpoint("during_shards")
            failpoint("after_shards")
            decisions = dict(con.sql("SELECT decision,count(*) FROM audit GROUP BY decision").fetchall())
        finally:
            with metrics.stage("database_flush_close"):
                con.close()
        with metrics.stage("temporary_artifact_cleanup"):
            (stage / "prepared.parquet").unlink()
            (stage / "working.duckdb").unlink()
            shutil.rmtree(stage / "spill", ignore_errors=True)
        with metrics.stage("artifact_hashing"):
            artifacts = [{"path": p.name, "sha256": digest(p), "bytes": p.stat().st_size}
                         for p in sorted(stage.iterdir()) if p.is_file()]
        body = {"schema_version": 1, "source_sha256": source_hash, "config": config,
                "records": count, "decisions": decisions, "artifacts": artifacts}
        dataset_id = hashlib.sha256(canonical(body)).hexdigest()
        manifest = {**body, "dataset_id": dataset_id}
        if (root / "tombstones" / f"{dataset_id}.json").exists():
            raise ValueError("cannot republish expired identity; use a revised processing version")
        with metrics.stage("publication"):
            failpoint("before_manifest")
            atomic(stage / "manifest.json", canonical(manifest))
            failpoint("after_manifest")
            for p in stage.iterdir():
                with p.open("rb") as f:
                    os.fsync(f.fileno())
            syncdir(stage)
            dest = releases / dataset_id
            if dest.exists():
                verify(root, dataset_id)
                shutil.rmtree(stage)
            else:
                os.rename(stage, dest)
                syncdir(releases)
            failpoint("before_pointer")
            atomic(root / "CURRENT", (dataset_id + "\n").encode())
            failpoint("after_pointer")
        return {"dataset_id": dataset_id, "records": count, "decisions": decisions,
                "elapsed_seconds": time.monotonic() - started, "measurements": metrics.report()}


def verify(root, dataset_id=None):
    root = Path(root)
    dataset_id = dataset_id or (root / "CURRENT").read_text().strip()
    if not re.fullmatch(r"[0-9a-f]{64}", dataset_id):
        raise ValueError("invalid dataset ID")
    if (root / "tombstones" / f"{dataset_id}.json").exists():
        raise ValueError("dataset has expired")
    folder = root / "releases" / dataset_id
    manifest = json.loads((folder / "manifest.json").read_bytes())
    body = {k: v for k, v in manifest.items() if k != "dataset_id"}
    if manifest.get("dataset_id") != dataset_id or hashlib.sha256(canonical(body)).hexdigest() != dataset_id:
        raise ValueError("manifest identity mismatch")
    for item in manifest["artifacts"]:
        if Path(item["path"]).name != item["path"]:
            raise ValueError("unsafe artifact path")
        p = folder / item["path"]
        if p.is_symlink() or p.stat().st_size != item["bytes"] or digest(p) != item["sha256"]:
            raise ValueError("artifact integrity failure: " + item["path"])
    return manifest
