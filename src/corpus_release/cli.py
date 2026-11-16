import argparse
import json
from .release import publish,verify
from .lifecycle import pin,unpin,expire,cleanup_staging,diff
from .reconcile import reconcile
from .lakebridge import publish_export


def main():
    p=argparse.ArgumentParser(description='Deterministic local corpus releases and independent consistency checks')
    sub=p.add_subparsers(dest='command',required=True)
    build=sub.add_parser('publish')
    build.add_argument('source')
    build.add_argument('root')
    build.add_argument('--license',required=True)
    build.add_argument('--revision',default='v1')
    build.add_argument('--shard-rows',type=int,default=100_000)
    build.add_argument('--profile', choices=['legacy-v1', 'nfc-lf-v2'], default='legacy-v1')
    exported=sub.add_parser('publish-export')
    exported.add_argument('export')
    exported.add_argument('root')
    exported.add_argument('--license',required=True)
    exported.add_argument('--revision',default='v1')
    for command in ('verify','reconcile'):
        check=sub.add_parser(command)
        check.add_argument('root')
        check.add_argument('--dataset-id')
    for command in ('pin','unpin'):
        action=sub.add_parser(command)
        action.add_argument('root')
        action.add_argument('dataset_id')
        action.add_argument('owner')
    action=sub.add_parser('expire')
    action.add_argument('root')
    action.add_argument('dataset_id')
    action=sub.add_parser('cleanup-staging')
    action.add_argument('root')
    action.add_argument('--min-age-seconds',type=float,default=86400)
    action.add_argument('--apply',action='store_true')
    action=sub.add_parser('diff')
    action.add_argument('root')
    action.add_argument('before')
    action.add_argument('after')
    a=p.parse_args()
    if a.command=='publish':
        result=publish(a.source,a.root,license=a.license,shard_rows=a.shard_rows,revision=a.revision,profile=a.profile)
    elif a.command=='publish-export':
        result=publish_export(a.export,a.root,license=a.license,revision=a.revision)
    elif a.command=='verify':
        result=verify(a.root,a.dataset_id)
    elif a.command=='reconcile':
        result=reconcile(a.root,a.dataset_id)
    elif a.command in ('pin','unpin'):
        (pin if a.command=='pin' else unpin)(a.root,a.dataset_id,a.owner)
        result={'action':a.command,'dataset_id':a.dataset_id,'owner':a.owner}
    elif a.command=='expire':
        result=expire(a.root,a.dataset_id)
    elif a.command=='cleanup-staging':
        result=cleanup_staging(a.root,min_age_seconds=a.min_age_seconds,dry_run=not a.apply)
    else:
        result=diff(a.root,a.before,a.after)
    print(json.dumps(result,sort_keys=True))
    if a.command=='reconcile' and not result['passed']:
        raise SystemExit(1)

if __name__=='__main__':
    main()
