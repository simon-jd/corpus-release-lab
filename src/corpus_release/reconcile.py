"""Independent SQL invariants over published artifacts, in addition to hashes."""
from pathlib import Path
import duckdb
from .release import verify,lock


def reconcile(root,dataset_id=None):
    root=Path(root)
    with lock(root):
        manifest=verify(root,dataset_id)
        folder=root/'releases'/manifest['dataset_id']
        files=[str(folder/a['path']) for a in manifest['artifacts'] if a['path'].startswith('shard-')]
        with duckdb.connect() as con:
            con.execute("SET memory_limit='1GB'")
            con.execute("SET threads=2")
            con.execute("SET preserve_insertion_order=false")
            con.read_parquet(str(folder/'audit.parquet')).create_view('audit')
            if files:
                con.read_parquet(files).create_view('samples')
            else:
                con.execute('CREATE TABLE samples(sample_id VARCHAR,text VARCHAR,split VARCHAR,source_line BIGINT,record_id VARCHAR)')
            checks={
                'manifest_record_count':con.sql('SELECT count(*) FROM samples').fetchone()[0]==manifest['records'],
                'unique_sample_ids':con.sql('SELECT count(*)-count(DISTINCT sample_id) FROM samples').fetchone()[0]==0,
                'valid_split_values':con.sql("SELECT count(*) FROM samples WHERE split NOT IN ('train','validation','test') OR split IS NULL").fetchone()[0]==0,
                'normalized_text_hashes':con.sql('SELECT count(*) FROM samples WHERE sha256(text)!=sample_id OR text IS NULL OR sample_id IS NULL').fetchone()[0]==0,
                'audit_source_line_uniqueness':con.sql('SELECT count(*)-count(DISTINCT line) FROM audit').fetchone()[0]==0,
                'accepted_audit_bijection':con.sql("""SELECT count(*) FROM (SELECT line,content_hash,requested_split FROM audit WHERE decision='accepted') a
                    FULL JOIN samples s ON a.line=s.source_line
                    WHERE a.line IS NULL OR s.source_line IS NULL OR a.content_hash!=s.sample_id OR a.requested_split!=s.split""").fetchone()[0]==0,
            }
            decisions=dict(con.sql('SELECT decision,count(*) FROM audit GROUP BY decision').fetchall())
            checks['decision_counts']=decisions==manifest['decisions']
            with (folder/'source.jsonl').open('rb') as source:
                source_lines=sum(1 for _ in source)
            checks['source_line_coverage']=con.sql('SELECT count(*) FROM audit').fetchone()[0]==source_lines and con.execute('SELECT count(*) FROM audit WHERE line<1 OR line>?',[source_lines]).fetchone()[0]==0
            leakage=con.sql('SELECT sample_id FROM samples GROUP BY sample_id HAVING count(DISTINCT split)>1').fetchall()
            checks['no_exact_cross_split_leakage']=not leakage
            rows=con.sql('SELECT count(*) FROM samples').fetchone()[0]
    return {'dataset_id':manifest['dataset_id'],'passed':all(checks.values()),'checks':checks,'records':rows,
            'source_lines':source_lines,'decisions':decisions,'cross_split_leaked_hashes':[row[0] for row in leakage],
            'rejected_fraction':sum(n for reason,n in decisions.items() if reason not in ('accepted','exact_duplicate'))/source_lines if source_lines else 0,
            'exact_duplicate_fraction':decisions.get('exact_duplicate',0)/source_lines if source_lines else 0}
