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


def normalize(text):
    return " ".join(unicodedata.normalize("NFKC", text).split())


def _prepare(source, target):
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
                    if split not in ("train", "validation", "test"):
                        raise ValueError("invalid_split")
                    text = normalize(obj["text"])
                    text.encode("utf-8", errors="strict")
                    changed = text != obj["text"]
                    if not text:
                        reason = "empty"
                    elif "\ufffd" in text:
                        reason = "replacement_character"
                    elif len(text) < 20:
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


def publish(source, root, *, license, seed=42, shard_rows=100_000):
    if not license or shard_rows < 1:
        raise ValueError("license and positive shard_rows required")
    root, source = Path(root), Path(source)
    started = time.monotonic()
    with lock(root):
        releases = root / "releases"
        releases.mkdir(exist_ok=True)
        stage = Path(tempfile.mkdtemp(prefix=".staging-", dir=root))
        # Partial stages are retained on failure for diagnosis; never discoverable by readers.
        shutil.copyfile(source, stage / "source.jsonl")
        source_hash = digest(stage / "source.jsonl")
        config = {"contract": 1, "rules": "nfkc-whitespace-min20-v1", "seed": seed,
                  "tokenizer": None, "shard_rows": shard_rows, "license": license,
                  "split_policy": "explicit-evaluation-priority-v1",
                  "duckdb": duckdb.__version__, "pyarrow": pa.__version__}
        _prepare(stage / "source.jsonl", stage / "prepared.parquet")
        con = duckdb.connect(str(stage / "working.duckdb"))
        try:
            con.execute("SET threads=2")
            con.execute("SET enable_progress_bar=false")
            con.execute("SET preserve_insertion_order=false")
            con.execute("SET memory_limit='1GB'")
            con.execute("SET temp_directory=?", [str(stage / "spill")])
            con.execute("CREATE TABLE raw AS SELECT * FROM read_parquet(?)", [str(stage / "prepared.parquet")])
            # Evaluation wins exact duplicate groups; train never contains evaluation text.
            con.execute("""CREATE TABLE ranked AS SELECT * EXCLUDE(text), row_number() OVER (
                PARTITION BY content_hash ORDER BY CASE requested_split
                WHEN 'test' THEN 0 WHEN 'validation' THEN 1 ELSE 2 END, line) AS rank
                FROM raw WHERE reason=''""")
            con.execute("""CREATE TABLE audit AS SELECT r.* EXCLUDE(text),
                CASE WHEN r.reason!='' THEN r.reason WHEN k.rank>1 THEN 'exact_duplicate'
                ELSE 'accepted' END AS decision
                FROM raw r LEFT JOIN ranked k USING(line)""")
            con.sql("SELECT * FROM audit ORDER BY line").write_parquet(str(stage / "audit.parquet"), compression="zstd", row_group_size=2048)
            con.execute("""CREATE TABLE accepted AS SELECT k.content_hash AS sample_id,
                k.requested_split AS split, k.line AS source_line, k.record_id,
                row_number() OVER (ORDER BY k.content_hash) AS ordinal
                FROM ranked k WHERE k.rank=1""")
            count = con.sql("SELECT count(*) FROM accepted").fetchone()[0]
            failpoint("before_shards")
            for start in range(0, count, shard_rows):
                path = stage / f"shard-{start // shard_rows:05d}.parquet"
                con.sql(f"SELECT a.* EXCLUDE(ordinal), r.text FROM (SELECT * FROM accepted WHERE ordinal>{start} AND ordinal<={start+shard_rows}) a JOIN raw r ON a.source_line=r.line ORDER BY a.ordinal").write_parquet(str(path), compression="zstd", row_group_size=2048)
                failpoint("during_shards")
            failpoint("after_shards")
            decisions = dict(con.sql("SELECT decision,count(*) FROM audit GROUP BY decision").fetchall())
        finally:
            con.close()
        (stage / "prepared.parquet").unlink()
        (stage / "working.duckdb").unlink()
        shutil.rmtree(stage / "spill", ignore_errors=True)
        artifacts = [{"path": p.name, "sha256": digest(p), "bytes": p.stat().st_size}
                     for p in sorted(stage.iterdir()) if p.is_file()]
        body = {"schema_version": 1, "source_sha256": source_hash, "config": config,
                "records": count, "decisions": decisions, "artifacts": artifacts}
        dataset_id = hashlib.sha256(canonical(body)).hexdigest()
        manifest = {**body, "dataset_id": dataset_id}
        if (root / "tombstones" / f"{dataset_id}.json").exists():
            raise ValueError("cannot republish expired identity; use a revised processing version")
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
                "elapsed_seconds": time.monotonic() - started}


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
