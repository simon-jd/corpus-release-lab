import hashlib
import json
import pyarrow as pa
import pyarrow.parquet as pq
from corpus_release import publish
from corpus_release.release import canonical,digest
from corpus_release.reconcile import reconcile


def test_reconciliation_finds_checksumming_valid_wrong_results(tmp_path):
    source=tmp_path/'source'
    source.write_text('{"text":"A sufficiently long sentence for consistency."}\n')
    root=tmp_path/'catalog'
    result=publish(source,root,license='CC0-1.0')
    assert reconcile(root)['passed']
    folder=root/'releases'/result['dataset_id']
    shard=next(folder.glob('shard-*'))
    rows=pq.read_table(shard).to_pylist()
    rows[0]['text']='The text changed but sample hash was not recomputed.'
    pq.write_table(pa.Table.from_pylist(rows),shard)
    manifest=json.loads((folder/'manifest.json').read_bytes())
    for item in manifest['artifacts']:
        if item['path']==shard.name:
            item.update(sha256=digest(shard),bytes=shard.stat().st_size)
    manifest.pop('dataset_id')
    new_id=hashlib.sha256(canonical(manifest)).hexdigest()
    manifest['dataset_id']=new_id
    (folder/'manifest.json').write_bytes(canonical(manifest))
    folder.rename(folder.parent/new_id)
    (root/'CURRENT').write_text(new_id+'\n')
    report=reconcile(root)
    assert not report['passed']
    assert not report['checks']['normalized_text_hashes']
