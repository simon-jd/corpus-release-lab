import json
import os
from pathlib import Path
import subprocess
import sys

import pyarrow.parquet as pq
import pytest
from corpus_release import publish, verify


def fixture(path, suffix=""):
    records = [
        {"id": "train", "text": "A sufficiently long shared document.", "split": "train"},
        {"id": "eval", "text": "A sufficiently long shared document.", "split": "test"},
        {"id": "clean", "text": "  A   different document with whitespace. " + suffix},
        {"id": "empty", "text": " "},
        {"id": "short", "text": "short"},
        {"id": "unicode", "text": "bad replacement character \ufffd text"},
    ]
    path.write_bytes(b"".join(json.dumps(r).encode()+b"\n" for r in records) + b'\xff\n' + b'{partial\n')
    return path


def test_determinism_traceability_and_leakage(tmp_path):
    source = fixture(tmp_path / "source")
    root = tmp_path / "catalog"
    first = publish(source, root, license="CC0-1.0", shard_rows=1)
    second = publish(source, root, license="CC0-1.0", shard_rows=1)
    assert first["dataset_id"] == second["dataset_id"]
    m = verify(root)
    folder = root / "releases" / m["dataset_id"]
    assert (folder / "source.jsonl").read_bytes() == source.read_bytes()
    audit = pq.read_table(folder / "audit.parquet").to_pylist()
    assert len(audit) == 8
    assert audit[0]["decision"] == "exact_duplicate"
    assert audit[2]["changed"]
    assert audit[-2]["decision"] == "invalid_encoding"
    assert audit[-1]["decision"] == "invalid_record"
    rows = [r for p in sorted(folder.glob("shard-*.parquet")) for r in pq.read_table(p).to_pylist()]
    assert len(rows) == len({r["sample_id"] for r in rows}) == 2
    assert next(r for r in rows if r["record_id"] == "eval")["split"] == "test"


@pytest.mark.parametrize("point", ["before_shards", "during_shards", "after_shards", "before_manifest", "after_manifest", "before_pointer", "after_pointer"])
def test_real_process_death(tmp_path, point):
    root = tmp_path / "catalog"
    old = publish(fixture(tmp_path / "old"), root, license="CC0-1.0")
    source = fixture(tmp_path / "new", "Changed input")
    proc = subprocess.run([sys.executable, "-m", "corpus_release.cli", "publish", str(source), str(root), "--license", "CC0-1.0"], env={**os.environ, "CORPUS_KILL_AT": point, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")})
    assert proc.returncode == -9
    visible = verify(root)
    assert (visible["dataset_id"] == old["dataset_id"]) == (point != "after_pointer")
    recovered = publish(source, root, license="CC0-1.0")
    assert verify(root)["dataset_id"] == recovered["dataset_id"]


def test_corruption_and_version_identity(tmp_path):
    root = tmp_path / "catalog"
    source = fixture(tmp_path / "source")
    a = publish(source, root, license="CC0-1.0")
    b = publish(source, root, license="CC0-1.0", seed=43)
    assert a["dataset_id"] != b["dataset_id"]
    verify(root, a["dataset_id"])
    folder = root / "releases" / b["dataset_id"]
    shard = next(folder.glob("shard-*.parquet"))
    shard.write_bytes(b"corruption")
    with pytest.raises(ValueError, match="integrity"):
        verify(root)
    with pytest.raises(ValueError, match="invalid dataset"):
        verify(root, "../escape")


def test_concurrent_writers(tmp_path):
    root = tmp_path / "catalog"
    paths = [fixture(tmp_path / f"source-{i}", str(i)) for i in range(2)]
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")}
    ps = [subprocess.Popen([sys.executable, "-m", "corpus_release.cli", "publish", str(p), str(root), "--license", "CC0-1.0"], env=env, stdout=subprocess.PIPE) for p in paths]
    results = [json.loads(p.communicate(timeout=60)[0]) for p in ps]
    assert all(p.returncode == 0 for p in ps)
    assert verify(root)["dataset_id"] in {r["dataset_id"] for r in results}
    for result in results:
        verify(root, result["dataset_id"])


def test_all_rejected(tmp_path):
    source = tmp_path / "empty"
    source.write_text('{"text":""}\n')
    result = publish(source, tmp_path / "catalog", license="CC0-1.0")
    assert result["records"] == 0
    assert verify(tmp_path / "catalog")["decisions"] == {"empty": 1}


def test_retention_pins_expiration_and_diff(tmp_path):
    from corpus_release.lifecycle import pin, unpin, expire, diff
    root = tmp_path / "catalog"
    a = publish(fixture(tmp_path / "source"), root, license="CC0-1.0")["dataset_id"]
    b = publish(fixture(tmp_path / "source", "new"), root, license="CC0-1.0")["dataset_id"]
    delta = diff(root, a, b)
    assert len(delta["added"]) == len(delta["removed"]) == 1
    assert delta["unchanged"] == 1
    with pytest.raises(ValueError, match="CURRENT"):
        expire(root, b)
    pin(root, a, "training-1")
    with pytest.raises(ValueError, match="pinned"):
        expire(root, a)
    unpin(root, a, "training-1")
    expire(root, a)
    with pytest.raises(ValueError, match="expired"):
        verify(root, a)
    with pytest.raises(ValueError, match="expired"):
        publish(fixture(tmp_path / "source"), root, license="CC0-1.0")


@pytest.mark.parametrize('point',['after_tombstone','during_expiration','after_expiration'])
def test_expiration_real_crash_resumes(tmp_path,point):
    from corpus_release.lifecycle import expire
    root=tmp_path/'catalog'
    source=fixture(tmp_path/'source')
    old=publish(source,root,license='CC0-1.0')['dataset_id']
    new=publish(source,root,license='CC0-1.0',revision='review-v2')['dataset_id']
    code='from corpus_release.lifecycle import expire; import sys; expire(sys.argv[1],sys.argv[2])'
    proc=subprocess.run([sys.executable,'-c',code,str(root),old],env={**os.environ,'CORPUS_KILL_AT':point,'PYTHONPATH':str(Path(__file__).resolve().parents[1]/'src')})
    assert proc.returncode==-9
    assert verify(root)['dataset_id']==new
    with pytest.raises(ValueError,match='expired'):
        verify(root,old)
    assert expire(root,old)['resumed']
    assert not (root/'releases'/old).exists()
    assert expire(root,old)['expired']


def test_staging_cleanup_and_provenance_identity(tmp_path):
    from corpus_release.lifecycle import cleanup_staging
    source=fixture(tmp_path/'source')
    root=tmp_path/'catalog'
    first=publish(source,root,license='CC0-1.0',provenance={'release':'parent-1'})
    second=publish(source,root,license='CC0-1.0',provenance={'release':'parent-2'})
    assert first['dataset_id']!=second['dataset_id']
    stage=root/'.staging-interrupted'
    stage.mkdir()
    (stage/'partial').write_text('unfinished')
    assert cleanup_staging(root,min_age_seconds=0)['staging_directories']==[stage.name]
    assert stage.exists()
    cleanup_staging(root,min_age_seconds=0,dry_run=False)
    assert not stage.exists()
    verify(root,first['dataset_id'])
    assert verify(root)['config']['source_provenance']=={'release':'parent-2'}


def test_measurements_do_not_change_identity(tmp_path):
    source=fixture(tmp_path/'source')
    root=tmp_path/'catalog'
    a=publish(source,root,license='CC0-1.0')
    b=publish(source,root,license='CC0-1.0')
    assert a['dataset_id']==b['dataset_id']
    stages={r['stage']:r for r in a['measurements']['stages']}
    assert stages['normalization']['input_artifact_bytes']==source.stat().st_size
    assert stages['normalization']['output_artifact_bytes']>0
    assert all(r['elapsed_seconds']>=0 and r['status']=='success' for r in stages.values())
    assert 'measurements' not in verify(root)


@pytest.mark.parametrize('point',['before_shards','during_shards','after_shards','before_manifest','after_manifest','before_pointer','after_pointer'])
def test_fresh_catalog_never_discovers_incomplete_stage(tmp_path,point):
    source=fixture(tmp_path/'source')
    root=tmp_path/'catalog'
    proc=subprocess.run([sys.executable,'-m','corpus_release.cli','publish',str(source),str(root),'--license','CC0-1.0'],env={**os.environ,'CORPUS_KILL_AT':point,'PYTHONPATH':str(Path(__file__).resolve().parents[1]/'src')})
    assert proc.returncode==-9
    if point=='after_pointer':
        assert verify(root)['records']==2
    else:
        with pytest.raises(FileNotFoundError): verify(root)
    result=publish(source,root,license='CC0-1.0')
    assert verify(root)['dataset_id']==result['dataset_id']
