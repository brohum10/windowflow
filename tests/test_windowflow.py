import concurrent.futures
import contextlib
import io
import json
from pathlib import Path
import random
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from windowflow import MAX_INT, Store, main


def event(i, ts, value=1, key='api'):
    return dict(id=str(i), key=key, timestamp_ms=ts, value=value)


class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)/'state.sqlite'
        self.store = Store(self.path, window_ms=100, lateness_ms=50)
        self.addCleanup(self.store.close)

    def test_window_boundaries(self):
        self.store.ingest([event(1,0),event(2,99),event(3,100)])
        self.assertEqual([r['count'] for r in self.store.report()['windows']], [2,1])

    def test_out_of_order_open_window(self):
        self.store.ingest([event(1,90),event(2,10)])
        self.assertEqual(self.store.report()['windows'][0]['count'],2)

    def test_finalization_and_late_side_output(self):
        self.store.ingest([event(1,10),event(2,150),event(3,99)])
        report = self.store.report()
        self.assertEqual(report['windows'][0]['finalized'],1)
        self.assertEqual([r['id'] for r in report['late_events']],['3'])
        self.assertEqual(report['windows'][0]['total'],1)

    def test_duplicate_after_restart(self):
        self.store.ingest([event(1,10)])
        other = Store(self.path)
        try:
            self.assertEqual(other.ingest([event(1,10)])['duplicates'],1)
            self.assertEqual(other.report()['windows'][0]['count'],1)
        finally:
            other.close()

    def test_duplicate_late_event(self):
        self.store.advance(100)
        self.store.ingest([event(1,10)])
        self.assertEqual(self.store.ingest([event(1,10)])['duplicates'],1)
        self.assertEqual(len(self.store.report()['late_events']),1)

    def test_conflict_rolls_back_entire_batch(self):
        self.store.ingest([event(1,10)])
        before = self.store.report()
        with self.assertRaises(ValueError):
            self.store.ingest([event(2,200),event(1,10,99)])
        self.assertEqual(before,self.store.report())

    def test_injected_storage_failure_atomicity(self):
        self.store.db.execute("CREATE TRIGGER fail_insert BEFORE INSERT ON events WHEN NEW.id='fail' BEGIN SELECT RAISE(ABORT,'injected'); END")
        before = self.store.report()
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.ingest([event(1,10),event('fail',300)])
        self.assertEqual(before,self.store.report())

    def test_process_exit_rolls_back_uncommitted_state(self):
        code = "import sqlite3,os,sys; d=sqlite3.connect(sys.argv[1]); d.execute('BEGIN IMMEDIATE'); d.execute('UPDATE config SET watermark=999 WHERE id=1'); os._exit(7)"
        p = subprocess.run([sys.executable,'-c',code,str(self.path)],check=False)
        self.assertEqual(p.returncode,7)
        self.assertEqual(self.store.report()['config']['watermark'],0)

    def test_concurrent_duplicate_delivery(self):
        def worker(_):
            s = Store(self.path)
            try: return s.ingest([event(1,10)])
            finally: s.close()
        with concurrent.futures.ThreadPoolExecutor(8) as pool:
            results = list(pool.map(worker,range(24)))
        self.assertEqual(sum(r['accepted'] for r in results),1)
        self.assertEqual(sum(r['duplicates'] for r in results),23)

    def test_concurrent_unique_delivery(self):
        def worker(i):
            s = Store(self.path)
            try: s.ingest([event(i,10)])
            finally: s.close()
        with concurrent.futures.ThreadPoolExecutor(8) as pool:
            list(pool.map(worker,range(40)))
        self.assertEqual(self.store.report()['windows'][0]['total'],40)

    def test_explicit_watermark_monotonic(self):
        self.store.advance(1000)
        with self.assertRaises(ValueError): self.store.advance(999)
        self.assertEqual(self.store.ingest([event(1,10)])['late'],1)

    def test_overflow_rolls_back(self):
        with self.assertRaises(ValueError):
            self.store.ingest([event(1,10,MAX_INT),event(2,11,1)])
        self.assertEqual(self.store.report()['windows'],[])

    def test_validation(self):
        for value in [event(1,True),event(1,-1),event(1,10,1.5),{},event('',10),event(1,MAX_INT)]:
            with self.assertRaises(ValueError): self.store.ingest([value])
        self.assertEqual(self.store.report()['windows'],[])

    def test_configuration_immutable(self):
        with self.assertRaises(ValueError): Store(self.path,window_ms=200,lateness_ms=50)
        self.assertEqual(self.store.report()['config']['window_ms'],100)

    def test_seeded_batch_partition_equivalence(self):
        rng = random.Random(42)
        events = [event(i,rng.randrange(500),rng.randrange(-5,6)) for i in range(200)]
        self.store.ingest(events)
        other = Store(Path(self.tmp.name)/'other.sqlite',window_ms=100,lateness_ms=50)
        try:
            for e in events: other.ingest([e])
            self.assertEqual(self.store.report(),other.report())
        finally: other.close()

    def test_offline_aggregation_oracle(self):
        rng = random.Random(19)
        items = [event(i,rng.randrange(500),rng.randrange(-5,6),str(i%3)) for i in range(1000)]
        s = Store(Path(self.tmp.name)/'oracle.sqlite',window_ms=100,lateness_ms=1000)
        try:
            s.ingest(items)
            expected = {}
            for e in items:
                key = (e['key'],e['timestamp_ms']//100*100)
                count,total = expected.get(key,(0,0))
                expected[key] = count+1,total+e['value']
            self.assertEqual({(r['key'],r['start']):(r['count'],r['total']) for r in s.report()['windows']},expected)
        finally: s.close()

    def test_cli(self):
        path = Path(self.tmp.name)/'events.json'
        path.write_text(json.dumps([event(1,10)]))
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(main(['--db',str(self.path),'ingest',str(path)]),0)
        self.assertEqual(json.loads(out.getvalue())['accepted'],1)
        path.write_text('[{"id":"a","id":"b"}]')
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as e:
                main(['--db',str(self.path),'ingest',str(path)])
        self.assertEqual(e.exception.code,2)


if __name__ == '__main__': unittest.main()
