"""Read a LakeBridge release via its public snapshot pointers, then publish text.

Spark and LakeBridge belong to the upstream runtime, not the DuckDB publisher's
Python environment. A durable JSONL/receipt boundary keeps both version sets intact.
"""
import hashlib
import json
import os
import platform
from pathlib import Path
import re

from .release import canonical, digest, publish, atomic


def export_release(spark, control, output, *, release_id, table='detail', id_columns=('tenant_id','event_id'), text_columns=('tenant_id','event_id','units'), exclude_tombstones=True, upstream_code=None):
    from pyspark.sql import functions as F
    if table not in ('detail','plans','daily') or not id_columns or not text_columns:
        raise ValueError('explicit table, ID and text columns required')
    release=control.release(release_id)
    if not release or release.get('release_id')!=release_id or release.get('validation',{}).get('passed') is not True:
        raise ValueError('a validated published LakeBridge release is required')
    pointer=release['tables'][table]
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){2}',pointer['table']) or type(pointer['snapshot_id']) is not int:
        raise ValueError('invalid LakeBridge table snapshot pointer')
    output=Path(output)
    output.mkdir(parents=True,exist_ok=False)
    frame=spark.read.option('snapshot-id',str(pointer['snapshot_id'])).table(pointer['table'])
    source_rows=frame.count()
    if exclude_tombstones:
        if 'is_deleted' not in frame.columns:
            raise ValueError('tombstone filtering requires is_deleted column')
        frame=frame.where(F.col('is_deleted')==False)  # noqa: E712; Spark column expression
    columns=tuple(dict.fromkeys((*id_columns,*text_columns)))
    if any(c not in frame.columns or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',c) for c in columns):
        raise ValueError('unknown or unsafe projection column')
    projected=frame.select(F.to_json(F.struct(*[F.col(c) for c in id_columns]), {'ignoreNullFields':'false'}).alias('id'),
                           F.to_json(F.struct(*[F.col(c) for c in text_columns]), {'ignoreNullFields':'false'}).alias('text')).orderBy('id')
    target=output/'source.jsonl'
    previous=None
    count=0
    with target.open('wb') as f:
        for row in projected.toLocalIterator():
            if row.id==previous or any(v is None for v in json.loads(row.id).values()):
                raise ValueError('projection IDs must be nonnull and unique')
            previous=row.id
            f.write(canonical({'id':row.id,'text':row.text,'split':'train'})+b'\n')
            count+=1
        f.flush()
        os.fsync(f.fileno())
    receipt={'schema_version':1,'adapter':'lakebridge-snapshot-json-v1','release':release,
             'release_sha256':hashlib.sha256(canonical(release)).hexdigest(),
             'selected_table':table,'pointer':pointer,'id_columns':list(id_columns),'text_columns':list(text_columns),
             'exclude_tombstones':exclude_tombstones,'source_snapshot_rows':source_rows,'exported_rows':count,
             'source_file':'source.jsonl','source_sha256':digest(target),'source_bytes':target.stat().st_size,
             'upstream_code':upstream_code,'upstream_runtime':{'spark':spark.version,'python':platform.python_version()}}
    atomic(output/'receipt.json',canonical(receipt))
    return receipt


def publish_export(export, catalog, *, license, revision='v1'):
    export=Path(export)
    receipt=json.loads((export/'receipt.json').read_bytes())
    if receipt.get('schema_version')!=1 or receipt.get('adapter')!='lakebridge-snapshot-json-v1':
        raise ValueError('unsupported LakeBridge export receipt')
    if receipt.get('source_file')!='source.jsonl':
        raise ValueError('unexpected exported source filename')
    release=receipt['release']
    if hashlib.sha256(canonical(release)).hexdigest()!=receipt['release_sha256'] or release.get('validation',{}).get('passed') is not True:
        raise ValueError('invalid upstream release receipt')
    if release['tables'][receipt['selected_table']]!=receipt['pointer']:
        raise ValueError('snapshot pointer does not match upstream release')
    source=export/'source.jsonl'
    if source.is_symlink() or source.stat().st_size!=receipt['source_bytes'] or digest(source)!=receipt['source_sha256']:
        raise ValueError('LakeBridge export integrity mismatch')
    # Copy provenance itself into the content-addressed manifest for portable lineage.
    return publish(source,catalog,license=license,revision=revision,provenance=receipt,expected_source_sha256=receipt["source_sha256"])
