"""Run in LakeBridge's pinned Spark environment; writes only --work."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

p=argparse.ArgumentParser()
p.add_argument('--lakebridge',type=Path,required=True)
p.add_argument('--work',type=Path,required=True)
a=p.parse_args()
sys.path.insert(0,str(a.lakebridge/'src'))
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from lakebridge.config import Settings,spark_session
from lakebridge.control import Control
from lakebridge.generate import generate
from lakebridge.pipeline import Pipeline
from corpus_release.lakebridge import export_release

if a.work.exists(): raise SystemExit('Use a fresh integration work directory.')
a.work.mkdir(parents=True)
settings=Settings(a.work/'lakebridge-runtime','corpus_fixture')
spark=spark_session(settings)
pipeline=Pipeline(spark,settings)
try:
    manifest=generate(a.work/'upstream-source',rows=100,tenants=5,days=7)
    first=pipeline.execute(manifest,1,reason='Corpus adapter qualification',run_id='corpus_initial')
    code={str(p.relative_to(a.lakebridge)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((a.lakebridge/'src/lakebridge').glob('*.py'))}
    # Read-only control connection: this consumer does not change LakeBridge publication state.
    control=Control(settings.root,read_only=True)
    try:
        before=control.status()
        one=export_release(spark,control,a.work/'export-first',release_id=first['release_id'],upstream_code=code)
        assert control.status()==before
        second=pipeline.execute(manifest,2,reason='Later revisions for snapshot isolation probe',run_id='corpus_later')
        two=export_release(spark,control,a.work/'export-old-again',release_id=first['release_id'],upstream_code=code)
        three=export_release(spark,control,a.work/'export-later',release_id=second['release_id'],upstream_code=code)
        assert one['source_sha256']==two['source_sha256']
        assert one['pointer']==two['pointer']
        assert one['pointer']!=three['pointer']
        assert one['source_sha256']!=three['source_sha256']
        report={'first':one,'old_after_new_release':two,'later':three,
                'invariants':{'old_snapshot_export_stable':True,'new_snapshot_changes_export':True,'read_only_control_unchanged_during_export':True},
                'runtime':'real local Spark 3.5.9 + Iceberg 1.10.2 via LakeBridge; synthetic event text projection'}
        (a.work/'upstream-report.json').write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps({'rows':one['exported_rows'],'first_snapshot':one['pointer'],'later_snapshot':three['pointer'],'invariants':report['invariants']}))
    finally:
        control.close()
finally:
    pipeline.close()
    spark.stop()
