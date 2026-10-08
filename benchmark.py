"""Synthetic durable ingest benchmark; includes commits, excludes network transport."""
import argparse
import json
from pathlib import Path
import platform
import statistics
import tempfile
import time
from windowflow import Store


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--events', type=int, default=10000)
    p.add_argument('--runs', type=int, default=3)
    p.add_argument('--batch-size', type=int, default=250)
    p.add_argument('--output', type=Path)
    args = p.parse_args()
    if min(args.events,args.runs,args.batch_size) < 1:
        p.error('all sizes must be positive')
    items = [dict(id=str(i),key=f'service-{i%10}',timestamp_ms=i,value=i%100) for i in range(args.events)]
    samples = []
    for _ in range(args.runs):
        with tempfile.TemporaryDirectory() as folder:
            s = Store(Path(folder)/'bench.sqlite',window_ms=1000,lateness_ms=500)
            try:
                start = time.perf_counter()
                accepted = 0
                for offset in range(0,len(items),args.batch_size):
                    accepted += s.ingest(items[offset:offset+args.batch_size])['accepted']
                ingest_s = time.perf_counter()-start
                before = s.report()
                start = time.perf_counter()
                duplicates = 0
                for offset in range(0,len(items),args.batch_size):
                    duplicates += s.ingest(items[offset:offset+args.batch_size])['duplicates']
                replay_s = time.perf_counter()-start
                if accepted != args.events or duplicates != args.events or before != s.report():
                    raise RuntimeError('benchmark correctness check failed')
                samples.append({'ingest_seconds':ingest_s, 'events_per_second':args.events/ingest_s,
                                'replay_seconds':replay_s,'duplicate_events':duplicates})
            finally: s.close()
    report = {'workload':'synthetic ascending timestamps, 10 keys, integer values 0-99',
              'scope':'local SQLite WAL, synchronous=FULL, commits included; no network or downstream sink',
              'events':args.events,'batch_size':args.batch_size,'runs':args.runs,
              'python':platform.python_version(),'platform':platform.platform(),
              'median_events_per_second':statistics.median(s['events_per_second'] for s in samples),
              'samples':samples}
    text = json.dumps(report,indent=2,allow_nan=False)
    if args.output: args.output.write_text(text+'\n',encoding='utf-8')
    print(text)


if __name__ == '__main__': main()
