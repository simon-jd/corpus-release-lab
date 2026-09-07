"""Explicit retention and resumable deletion under the local catalog lock."""
import hashlib
import json
import math
from pathlib import Path
import re
import shutil
import time
from .release import atomic, canonical, failpoint, lock, syncdir, verify


def valid_id(dataset_id):
    if not isinstance(dataset_id,str) or not re.fullmatch(r'[0-9a-f]{64}',dataset_id):
        raise ValueError('invalid dataset ID')


def valid_owner(owner):
    if not isinstance(owner,str) or not re.fullmatch(r'[A-Za-z0-9_-]+',owner):
        raise ValueError('owner must be alphanumeric with hyphens or underscores')


def pin(root, dataset_id, owner):
    valid_owner(owner)
    root=Path(root)
    with lock(root):
        verify(root,dataset_id)
        pins=root/'pins'/dataset_id
        pins.mkdir(parents=True,exist_ok=True)
        syncdir(root)
        syncdir(root/'pins')
        atomic(pins/owner,b'pinned\n')


def unpin(root,dataset_id,owner):
    valid_id(dataset_id)
    valid_owner(owner)
    root=Path(root)
    with lock(root):
        pins=root/'pins'/dataset_id
        if pins.exists():
            (pins/owner).unlink(missing_ok=True)
            syncdir(pins)


def expire(root,dataset_id):
    valid_id(dataset_id)
    root=Path(root)
    with lock(root):
        if (root/'CURRENT').read_text().strip()==dataset_id:
            raise ValueError('cannot expire CURRENT')
        pins=root/'pins'/dataset_id
        if pins.exists() and any(pins.iterdir()):
            raise ValueError('dataset is pinned')
        tombstones=root/'tombstones'
        tombstone=tombstones/f'{dataset_id}.json'
        folder=root/'releases'/dataset_id
        if folder.is_symlink():
            raise ValueError('release directory cannot be a symlink')
        resumed=tombstone.exists()
        if resumed:
            manifest=json.loads(tombstone.read_bytes())
            body={k:v for k,v in manifest.items() if k!='dataset_id'}
            if manifest.get('dataset_id')!=dataset_id or hashlib.sha256(canonical(body)).hexdigest()!=dataset_id:
                raise ValueError('tombstone identity mismatch')
        else:
            manifest=verify(root,dataset_id)
            tombstones.mkdir(exist_ok=True)
            syncdir(root)
            atomic(tombstone,canonical(manifest))
        # Tombstone is the expiration linearization point. Retry does not require remaining files.
        failpoint('after_tombstone')
        if folder.exists():
            for path in sorted(folder.iterdir()):
                if path.is_dir() and not path.is_symlink():
                    raise ValueError('unexpected nested release directory')
                path.unlink()
                failpoint('during_expiration')
            folder.rmdir()
            syncdir(root/'releases')
        failpoint('after_expiration')
        return {'dataset_id':dataset_id,'expired':True,'resumed':resumed}


def cleanup_staging(root, *, min_age_seconds=86400, dry_run=True):
    if type(min_age_seconds) not in (int,float) or not math.isfinite(min_age_seconds) or min_age_seconds < 0:
        raise ValueError('min_age_seconds must be finite and nonnegative')
    root=Path(root)
    with lock(root):
        cutoff=time.time()-min_age_seconds
        paths=[p for p in sorted(root.glob('.staging-*')) if p.stat().st_mtime<=cutoff]
        if any(p.is_symlink() or not p.is_dir() for p in paths):
            raise ValueError('unexpected staging path')
        report={'dry_run':dry_run,'staging_directories':[p.name for p in paths]}
        if not dry_run:
            for path in paths:
                shutil.rmtree(path)
            syncdir(root)
        return report


def diff(root,before,after):
    import duckdb
    root=Path(root)
    # Hold lock so expiration cannot race comparisons.
    with lock(root):
        verify(root,before)
        verify(root,after)
        with duckdb.connect() as con:
            def samples(version):
                files=sorted((root/'releases'/version).glob('shard-*.parquet'))
                if not files:
                    return set()
                return set(con.execute('SELECT sample_id,split FROM read_parquet(?)',[[str(p) for p in files]]).fetchall())
            a,b=samples(before),samples(after)
    return {'before':before,'after':after,'added':sorted(b-a),'removed':sorted(a-b),'unchanged':len(a&b)}
