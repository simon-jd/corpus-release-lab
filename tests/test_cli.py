import json
from pathlib import Path
import os
import subprocess
import sys


def test_cli_lifecycle_roundtrip(tmp_path):
    source=tmp_path/'input.jsonl'
    source.write_text('{"text":"A complete synthetic sentence for CLI qualification."}\n')
    root=tmp_path/'catalog'
    def command(*args):
        result=subprocess.run([sys.executable,'-m','corpus_release.cli',*map(str,args)],capture_output=True,text=True,env={**os.environ,'PYTHONPATH':str(Path(__file__).resolve().parents[1]/'src')})
        assert result.returncode==0,result.stderr
        return json.loads(result.stdout)
    first=command('publish',source,root,'--license','CC0-1.0')['dataset_id']
    second=command('publish',source,root,'--license','CC0-1.0','--revision','v2')['dataset_id']
    assert command('reconcile',root)['passed']
    assert command('diff',root,first,second)['unchanged']==1
    command('pin',root,first,'reader1')
    command('unpin',root,first,'reader1')
    assert command('expire',root,first)['expired']
    assert command('cleanup-staging',root)['dry_run']
    assert command('verify',root)['dataset_id']==second
