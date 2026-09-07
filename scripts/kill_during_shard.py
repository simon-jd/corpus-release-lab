"""Kill a real large writer while an actual Parquet shard is incomplete."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import pyarrow.parquet as pq
from corpus_release import publish,verify
from corpus_release.release import digest

p=argparse.ArgumentParser()
p.add_argument('--source',type=Path,required=True)
p.add_argument('--work',type=Path,required=True)
p.add_argument('--expected-id',required=True)
p.add_argument('--output',type=Path,required=True)
a=p.parse_args()
if a.work.exists(): raise SystemExit('Use a fresh work directory.')
a.work.mkdir(parents=True)
old_source=a.work/'old.jsonl'
old_source.write_text('{"text":"The prior release remains available during the next publication."}\n')
old=publish(old_source,a.work/'catalog',license='CC0-1.0')
command=[sys.executable,'-m','corpus_release.cli','publish',str(a.source),str(a.work/'catalog'),'--license','CC0-1.0']
start=time.monotonic()
with (a.work/'killed.stdout').open('w') as stdout,(a.work/'killed.stderr').open('w') as stderr:
    proc=subprocess.Popen(command,stdout=stdout,stderr=stderr)
    found=None
    try:
        while time.monotonic()-start < 120:
            if proc.poll() is not None: raise RuntimeError('writer exited before incomplete shard was observed')
            files=list((a.work/'catalog').glob('.staging-*/shard-*.parquet'))
            if files:
                shard=files[0]
                size=shard.stat().st_size
                if size:
                    try:
                        pq.ParquetFile(shard)
                    except Exception as exc:
                        found={'path':str(shard.relative_to(a.work)),'bytes_before_kill':size,'observed_parquet_error':type(exc).__name__}
                        os.kill(proc.pid,signal.SIGKILL)
                        break
            time.sleep(.005)
        if found is None: raise RuntimeError('did not observe an incomplete Parquet shard before deadline')
        assert proc.wait(timeout=10)==-9
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
assert verify(a.work/'catalog')['dataset_id']==old['dataset_id']
retry=subprocess.run(command,capture_output=True,text=True,check=True,timeout=180)
(a.work/'retry.stdout').write_text(retry.stdout)
(a.work/'retry.stderr').write_text(retry.stderr)
recovered=json.loads(retry.stdout)
assert recovered['dataset_id']==a.expected_id
assert verify(a.work/'catalog')['dataset_id']==a.expected_id
report={'schema_version':1,'source_sha256':digest(a.source),'source_bytes':a.source.stat().st_size,
        'observed_incomplete_shard':found,'killed_exit_code':proc.returncode,'previous_release_remained_readable':True,
        'old_dataset_id':old['dataset_id'],'recovered_dataset_id':recovered['dataset_id'],'retry_matches_expected_release':True,
        'runtime_evidence_sha256':{p.name:digest(p) for p in a.work.glob('*.std*')}}
a.output.write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))
