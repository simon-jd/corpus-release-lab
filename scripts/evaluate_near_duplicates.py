"""Freeze a deterministic human-labeled PAWS sample and evaluate fixed rules."""
import argparse
import hashlib
import json
from pathlib import Path
import pyarrow.parquet as pq
from corpus_release.near_duplicates import evaluate
from corpus_release.release import digest

p=argparse.ArgumentParser()
p.add_argument('--raw',type=Path,default=Path('.run/paws'))
p.add_argument('--output',type=Path,default=Path('evidence/near-duplicates.json'))
p.add_argument('--fixture',type=Path,default=Path('fixtures/near-duplicates/paws-sample.json'))
p.add_argument('--from-fixture',action='store_true')
a=p.parse_args()
revision='161ece9501cf0a11f3e48bd356eaa82de46d6a09'
if a.from_fixture:
    sample=json.loads(a.fixture.read_text())
else:
    sample={'source':'Google LLC, PAWS-Wiki Labeled (Final)', 'human_label_provenance':'Published paraphrase/fluency judgments, not labels authored or independently reviewed by this project.',
            'repository':'https://github.com/google-research-datasets/paws',
            'mirror':'https://huggingface.co/datasets/google-research-datasets/paws', 'mirror_revision':revision,
            'license_file':'PAWS-LICENSE.txt', 'sampling':'lowest 200 SHA256(paws-audit-v1:<split>:<id>) per split, without label filtering',
            'splits':{},'raw_sources':{}}
    for split in ('validation','test'):
        path=a.raw/f'{split}.parquet'
        rows=pq.read_table(path).to_pylist()
        selected=sorted(rows,key=lambda r:hashlib.sha256(f'paws-audit-v1:{split}:{r["id"]}'.encode()).hexdigest())[:200]
        assert len(rows)==8000 and len(selected)==200
        sample['splits'][split]=selected
        sample['raw_sources'][split]={'url':f'https://huggingface.co/datasets/google-research-datasets/paws/resolve/{revision}/labeled_final/{split}-00000-of-00001.parquet',
                                      'sha256':digest(path),'rows':len(rows),'bytes':path.stat().st_size}
    a.fixture.parent.mkdir(parents=True,exist_ok=True)
    a.fixture.write_text(json.dumps(sample,indent=2,ensure_ascii=False)+'\n')
reports=[]
for split,pairs in sample['splits'].items():
    for width in (1,3):
        for threshold in (.5,.7,.8,.9,.95,1.):
            result=evaluate(pairs,width=width,threshold=threshold)
            predictions=result.pop('predictions')
            result['split']=split
            if threshold==.8:
                result['predictions']=predictions
                bad_ids={r['id'] for r in predictions if r['outcome']=='fp'}
                result['false_deletion_examples']=[r for r in pairs if r['id'] in bad_ids][:5]
            reports.append(result)
report={'schema_version':1,'fixture_sha256':digest(a.fixture),'source':{k:v for k,v in sample.items() if k!='splits'},
        'fixed_rule':'casefolded word 3-shingle Jaccard >= 0.8; sensitivity grid is diagnostic, no test-set threshold tuning',
        'reports':reports,'decision':'near_duplicate_filter_disabled; candidates are review-only',
        'limitations':['PAWS is an adversarial paraphrase benchmark, not a representative corpus-duplicate label distribution.',
                       'Precision/recall here use human semantic equivalence labels as a deletion-risk stress test, not a verified estimate of production deduplication quality.',
                       'Published human judgments were reused unchanged; no newly collected annotation or human project review is claimed.',
                       'No records are removed by this evaluator. A quality improvement cannot be claimed from reducing output size.']}
a.output.parent.mkdir(parents=True,exist_ok=True)
a.output.write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
print(json.dumps([{k:r[k] for k in ('split','shingle_width','threshold','tp','fp','fn','precision','recall')} for r in reports if r['threshold']==.8],indent=2))
