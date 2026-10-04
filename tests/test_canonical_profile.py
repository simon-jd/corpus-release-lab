import json
import random

import pyarrow.parquet as pq

from corpus_release import publish, verify
from corpus_release.reconcile import reconcile
from corpus_release.release import normalize


def test_nfc_lf_preserves_case_compatibility_characters_and_whitespace():
    assert normalize("Ａ Cafe\u0301\r\n  Case\tX\r", "nfc-lf-v2") == "Ａ Café\n  Case\tX\n"


def test_shuffle_stability_and_evaluation_priority(tmp_path):
    rows = [
        {"id": "z", "split": "train", "text": "Cafe\u0301\r\nshared"},
        {"id": "e", "split": "eval", "text": "Café\nshared"},
        {"id": "b", "split": "train", "text": "kept twice"},
        {"id": "a", "split": "train", "text": "kept twice"},
        {"id": "c", "split": "train", "text": "KEPT twice"},
    ]
    identities = []
    for seed in range(3):
        random.Random(seed).shuffle(rows)
        source = tmp_path / f"input-{seed}.jsonl"
        source.write_text("".join(json.dumps(row) + "\n" for row in rows))
        root = tmp_path / f"catalog-{seed}"
        result = publish(source, root, license="CC0-1.0", profile="nfc-lf-v2", shard_rows=2)
        identities.append(result["dataset_id"])
        assert reconcile(root)["passed"]
        manifest = verify(root)
        assert sum(manifest["decisions"].values()) == 5
        folder = root / "releases" / result["dataset_id"]
        actual = [row for path in sorted(folder.glob("shard-*.parquet")) for row in pq.read_table(path).to_pylist()]
        assert [row["record_id"] for row in actual] == ["e", "a", "c"]
        assert manifest["config"]["row_group_size"] == 4096
    assert len(set(identities)) == 1
