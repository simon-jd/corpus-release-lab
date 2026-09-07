"""Generate licensed synthetic JSONL and measure a real publication subprocess."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

p = argparse.ArgumentParser()
p.add_argument("--reuse-source", action="store_true")
p.add_argument("--bytes", type=int, default=1_073_741_824)
p.add_argument("--work", type=Path, default=Path(".run/scale"))
p.add_argument("--evidence", type=Path, default=Path("evidence/scale.json"))
a = p.parse_args()
if a.bytes < 1:
    p.error("--bytes must be positive")
a.work.mkdir(parents=True, exist_ok=True)
source = a.work / "synthetic.jsonl"
h = hashlib.sha256()
n = size = 0
if a.reuse_source:
    with source.open("rb") as f:
        for row in f:
            h.update(row)
            n += 1
            size += len(row)
if a.reuse_source and size < a.bytes:
    p.error("reused source is smaller than --bytes; refusing to invent additional rows")
with (open(os.devnull, "wb") if a.reuse_source else source.open("wb")) as f:
    while size < a.bytes:
        # Unique, moderately compressible content; not a natural language quality benchmark.
        text = " ".join(hashlib.sha256(f"{n}:{j}".encode()).hexdigest() for j in range(64))
        row = json.dumps({"id": str(n), "text": text, "split": "test" if n % 20 == 0 else "train"}).encode() + b"\n"
        f.write(row)
        h.update(row)
        n += 1
        size += len(row)
code = {str(f.relative_to(Path(__file__).resolve().parents[1])): hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted((Path(__file__).resolve().parents[1] / "src").rglob("*.py"))}
resource_file = a.work / "resources.json"
start = time.time()
command = [sys.executable, "-m", "corpus_release.cli", "publish", str(source), str(a.work / "catalog"), "--license", "CC0-1.0"]
result = subprocess.run(["/usr/bin/time", "-f", '{"peak_rss_kib":%M,"user_seconds":%U,"system_seconds":%S,"filesystem_inputs":%I,"filesystem_outputs":%O}', "-o", str(resource_file), *command], capture_output=True, text=True, env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")})
a.evidence.parent.mkdir(parents=True, exist_ok=True)
report = {"schema_version": 1, "source_code_sha256": code, "started_unix": start, "host": platform.platform(), "python": sys.version,
          "input_bytes": size, "input_records": n, "source_sha256": h.hexdigest(), "license": "CC0-1.0",
          "generator": "unique-sha256-words-v1", "command": command, "returncode": result.returncode,
          "stdout": result.stdout, "stderr": result.stderr, "resources": resource_file.read_text()}
if result.returncode == 0:
    data = json.loads(result.stdout)
    report["result"] = data
    report["valid_records_per_second"] = data["records"] / data["elapsed_seconds"]
a.evidence.write_text(json.dumps(report, indent=2)+"\n")
print(json.dumps(report, indent=2))
raise SystemExit(result.returncode)
