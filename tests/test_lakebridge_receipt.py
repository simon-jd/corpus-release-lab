import copy
import hashlib
import json
import pytest
from corpus_release.lakebridge import publish_export
from corpus_release.release import canonical,digest,verify


def export_fixture(path):
    path.mkdir()
    source=path/'source.jsonl'
    source.write_text('{"id":"one","text":"Synthetic source text from a released snapshot."}\n')
    release={'release_id':'fixture-release','validation':{'passed':True},'tables':{'detail':{'table':'lake.fixture.detail','snapshot_id':123}}}
    receipt={'schema_version':1,'adapter':'lakebridge-snapshot-json-v1','release':release,'release_sha256':hashlib.sha256(canonical(release)).hexdigest(),
             'selected_table':'detail','pointer':release['tables']['detail'],'source_file':'source.jsonl','source_sha256':digest(source),'source_bytes':source.stat().st_size}
    (path/'receipt.json').write_bytes(canonical(receipt))
    return receipt


def test_upstream_identity_bound_to_corpus_and_tampering_rejected(tmp_path):
    export=tmp_path/'export'
    receipt=export_fixture(export)
    root=tmp_path/'catalog'
    result=publish_export(export,root,license='CC0-1.0')
    manifest=verify(root)
    assert manifest['config']['source_provenance']['release_sha256']==receipt['release_sha256']
    bad=copy.deepcopy(receipt)
    bad['pointer']['snapshot_id']=456
    (export/'receipt.json').write_bytes(canonical(bad))
    with pytest.raises(ValueError): publish_export(export,root,license='CC0-1.0')
    (export/'receipt.json').write_bytes(canonical(receipt))
    (export/'source.jsonl').write_text('tampered')
    with pytest.raises(ValueError,match='integrity'): publish_export(export,root,license='CC0-1.0')
    assert verify(root)['dataset_id']==result['dataset_id']


def test_source_change_between_verification_and_snapshot_rejects(tmp_path,monkeypatch):
    import shutil
    export=tmp_path/'export'
    export_fixture(export)
    original=shutil.copyfile
    def racing_copy(source,target):
        source.write_text('{"text":"Changed after initial receipt verification, before snapshotting."}\n')
        return original(source,target)
    monkeypatch.setattr(shutil,'copyfile',racing_copy)
    with pytest.raises(ValueError,match='source changed'):
        publish_export(export,tmp_path/'catalog',license='CC0-1.0')
    assert not (tmp_path/'catalog'/'CURRENT').exists()
