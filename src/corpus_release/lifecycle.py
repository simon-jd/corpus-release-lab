"""Explicit retention operations. No automatic time-based deletion."""
import json
from pathlib import Path
import shutil
from .release import atomic, canonical, lock, syncdir, verify


def pin(root, dataset_id, owner):
    if not owner or not owner.replace("-", "").replace("_", "").isalnum():
        raise ValueError("owner must be alphanumeric with hyphens or underscores")
    root = Path(root)
    with lock(root):
        verify(root, dataset_id)
        pins = root / "pins" / dataset_id
        pins.mkdir(parents=True, exist_ok=True)
        atomic(pins / owner, b"pinned\n")


def unpin(root, dataset_id, owner):
    # Validate using the same path constraints; retained version must still exist.
    pin(root, dataset_id, owner)
    root = Path(root)
    with lock(root):
        (root / "pins" / dataset_id / owner).unlink()
        syncdir(root / "pins" / dataset_id)


def expire(root, dataset_id):
    root = Path(root)
    with lock(root):
        manifest = verify(root, dataset_id)
        if (root / "CURRENT").read_text().strip() == dataset_id:
            raise ValueError("cannot expire CURRENT")
        pins = root / "pins" / dataset_id
        if pins.exists() and any(pins.iterdir()):
            raise ValueError("dataset is pinned")
        tombstones = root / "tombstones"
        tombstones.mkdir(exist_ok=True)
        # Durable tombstone before rename: verify rejects at this point even if killed.
        atomic(tombstones / f"{dataset_id}.json", canonical(manifest))
        shutil.rmtree(root / "releases" / dataset_id)
        syncdir(root / "releases")


def diff(root, before, after):
    import duckdb
    root = Path(root)
    verify(root, before)
    verify(root, after)
    with duckdb.connect() as con:
        def samples(version):
            files = sorted((root / "releases" / version).glob("shard-*.parquet"))
            if not files:
                return set()
            return set(con.execute("SELECT sample_id, split FROM read_parquet(?)", [[str(p) for p in files]]).fetchall())
        a, b = samples(before), samples(after)
    return {"before": before, "after": after, "added": sorted(b-a), "removed": sorted(a-b), "unchanged": len(a & b)}
