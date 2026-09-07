import argparse
import json
from .release import publish, verify


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command", required=True)
    build = sub.add_parser("publish")
    build.add_argument("source")
    build.add_argument("root")
    build.add_argument("--license", required=True)
    build.add_argument("--shard-rows", type=int, default=100_000)
    check = sub.add_parser("verify")
    check.add_argument("root")
    check.add_argument("--dataset-id")
    a = p.parse_args()
    result = publish(a.source, a.root, license=a.license, shard_rows=a.shard_rows) if a.command == "publish" else verify(a.root, a.dataset_id)
    print(json.dumps(result, sort_keys=True))

if __name__ == "__main__":
    main()
