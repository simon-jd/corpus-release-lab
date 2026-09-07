"""Linux process I/O observations; independent of immutable dataset identity."""
from contextlib import contextmanager
import json
from pathlib import Path
import resource
import time


def counters():
    path = Path('/proc/self/io')
    if not path.exists():
        return None
    return {key: int(value) for key, value in (line.split(':') for line in path.read_text().splitlines())}


class Metrics:
    def __init__(self):
        self.stages = []

    @contextmanager
    def stage(self, name):
        before, start = counters(), time.perf_counter()
        item = {'stage': name, 'status': 'failed'}
        try:
            yield item
            item['status'] = 'success'
        finally:
            after = counters()
            item.update(elapsed_seconds=time.perf_counter()-start,
                        process_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                        process_io_delta={k: after[k]-before[k] for k in before} if before is not None and after is not None else None)
            self.stages.append(item)

    def report(self):
        return {'schema_version': 1, 'stages': self.stages,
                'definitions': {'rchar/wchar': 'Process syscall bytes, including cached reads, library metadata and observer overhead; not logical row sizes.',
                                'read_bytes/write_bytes': 'Linux process storage-layer I/O counters; cache hits may report zero. Not device-level total traffic.',
                                'process_peak_rss_kib': 'Process lifetime high-water mark at stage end, not incremental stage allocation.',
                                'artifact_bytes': 'Explicit measured source/output file sizes where included; distinct from storage-layer I/O.'}}
