"""Durable event-time windows on SQLite. No third-party runtime dependencies."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3

MAX_INT = (1 << 63) - 1


def integer(value, name, low=0, high=MAX_INT):
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f'{name} must be an integer in [{low}, {high}]')
    return value


def unique_fields(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'duplicate JSON field: {key}')
        result[key] = value
    return result


class Store:
    """One connection per worker. SQLite serializes writers across processes."""

    def __init__(self, path, *, window_ms=None, lateness_ms=None):
        self.path = Path(path)
        if window_ms is not None:
            integer(window_ms, 'window_ms', 1)
            integer(lateness_ms, 'lateness_ms')
        elif lateness_ms is not None:
            raise ValueError('lateness_ms requires window_ms')
        if window_ms is None and not self.path.is_file():
            raise ValueError('database does not exist; initialize it first')
        self.db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        try:
            self.db.execute('PRAGMA journal_mode=WAL')
            self.db.execute('PRAGMA synchronous=FULL')
            with self.transaction():
                if window_ms is not None:
                    self.db.execute('CREATE TABLE IF NOT EXISTS config (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL, window_ms INTEGER NOT NULL, lateness_ms INTEGER NOT NULL, max_seen INTEGER NOT NULL, watermark INTEGER NOT NULL)')
                    self.db.execute('CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, key TEXT NOT NULL, ts INTEGER NOT NULL, value INTEGER NOT NULL, status TEXT NOT NULL CHECK(status IN (\'accepted\',\'late\')))')
                    self.db.execute('CREATE TABLE IF NOT EXISTS windows (key TEXT NOT NULL, start INTEGER NOT NULL, end INTEGER NOT NULL, count INTEGER NOT NULL, total INTEGER NOT NULL, finalized INTEGER NOT NULL DEFAULT 0 CHECK(finalized IN (0,1)), PRIMARY KEY(key,start))')
                    self.db.execute('CREATE INDEX IF NOT EXISTS windows_open_end ON windows(end) WHERE finalized=0')
                    self.db.execute('INSERT OR IGNORE INTO config VALUES(1,1,?,?,-1,0)', (window_ms, lateness_ms))
                config = self.db.execute('SELECT * FROM config WHERE id=1').fetchone()
                if config is None or config['version'] != 1:
                    raise ValueError('unsupported database schema')
                if window_ms is not None and (config['window_ms'], config['lateness_ms']) != (window_ms, lateness_ms):
                    raise ValueError('existing database has different window configuration')
                self.width = config['window_ms']
                self.lateness = config['lateness_ms']
        except Exception:
            self.db.close()
            raise

    @contextmanager
    def transaction(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

    def close(self):
        self.db.close()

    def validate(self, event):
        if not isinstance(event, dict) or set(event) != {'id','key','timestamp_ms','value'}:
            raise ValueError('events require exactly id, key, timestamp_ms and value')
        for field in ('id', 'key'):
            if not isinstance(event[field], str) or not event[field].strip():
                raise ValueError(f'{field} must be a nonblank string')
        integer(event['timestamp_ms'], 'timestamp_ms', 0, MAX_INT-self.width)
        integer(event['value'], 'value', -MAX_INT, MAX_INT)

    def ingest(self, events):
        if not isinstance(events, list):
            raise ValueError('batch must be an array')
        for event in events:
            self.validate(event)
        counts = {'accepted': 0, 'late': 0, 'duplicates': 0}
        with self.transaction():
            config = self.db.execute('SELECT * FROM config WHERE id=1').fetchone()
            watermark, max_seen = config['watermark'], config['max_seen']
            for e in events:
                old = self.db.execute('SELECT * FROM events WHERE id=?', (e['id'],)).fetchone()
                if old is not None:
                    if (old['key'],old['ts'],old['value']) != (e['key'],e['timestamp_ms'],e['value']):
                        raise ValueError(f'event ID reused with different payload: {e["id"]}')
                    counts['duplicates'] += 1
                    continue
                start = e['timestamp_ms'] // self.width * self.width
                end = start + self.width
                status = 'late' if end <= watermark else 'accepted'
                self.db.execute('INSERT INTO events VALUES(?,?,?,?,?)', (e['id'],e['key'],e['timestamp_ms'],e['value'],status))
                counts[status] += 1
                if status == 'accepted':
                    prior = self.db.execute('SELECT count,total FROM windows WHERE key=? AND start=?', (e['key'], start)).fetchone()
                    count = (prior['count'] if prior else 0) + 1
                    total = (prior['total'] if prior else 0) + e['value']
                    integer(count, 'window count', 1)
                    integer(total, 'window total', -MAX_INT, MAX_INT)
                    self.db.execute('INSERT INTO windows(key,start,end,count,total) VALUES(?,?,?,?,?) ON CONFLICT(key,start) DO UPDATE SET count=excluded.count,total=excluded.total', (e['key'],start,end,count,total))
                    max_seen = max(max_seen,e['timestamp_ms'])
                    watermark = max(watermark, max_seen-self.lateness, 0)
            self.db.execute('UPDATE config SET watermark=?, max_seen=? WHERE id=1', (watermark,max_seen))
            self.db.execute('UPDATE windows SET finalized=1 WHERE finalized=0 AND end<=?', (watermark,))
        return {**counts, 'watermark_ms':watermark}

    def advance(self, watermark):
        integer(watermark, 'watermark_ms')
        with self.transaction():
            current = self.db.execute('SELECT watermark FROM config WHERE id=1').fetchone()[0]
            if watermark < current:
                raise ValueError('watermark cannot move backwards')
            self.db.execute('UPDATE config SET watermark=? WHERE id=1', (watermark,))
            self.db.execute('UPDATE windows SET finalized=1 WHERE finalized=0 AND end<=?', (watermark,))

    def report(self):
        # A deferred read transaction gives one consistent snapshot without reserving a writer.
        self.db.execute('BEGIN')
        try:
            config = dict(self.db.execute('SELECT * FROM config WHERE id=1').fetchone())
            windows = [dict(r) for r in self.db.execute('SELECT * FROM windows ORDER BY start,key')]
            late = [dict(r) for r in self.db.execute("SELECT id,key,ts,value FROM events WHERE status='late' ORDER BY ts,id")]
            self.db.execute('COMMIT')
            return {'config':config, 'windows':windows, 'late_events':late}
        except BaseException:
            self.db.execute('ROLLBACK')
            raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', type=Path, required=True)
    commands = parser.add_subparsers(dest='command', required=True)
    init = commands.add_parser('init')
    init.add_argument('--window-ms', type=int, required=True)
    init.add_argument('--lateness-ms', type=int, required=True)
    ingest = commands.add_parser('ingest')
    ingest.add_argument('file', type=Path, help='JSON array; one atomic batch')
    advance = commands.add_parser('advance')
    advance.add_argument('watermark_ms', type=int)
    commands.add_parser('report')
    args = parser.parse_args(argv)
    store = None
    try:
        store = Store(args.db, window_ms=args.window_ms, lateness_ms=args.lateness_ms) if args.command == 'init' else Store(args.db)
        if args.command == 'ingest':
            result = store.ingest(json.loads(args.file.read_text(encoding='utf-8'), object_pairs_hook=unique_fields))
        else:
            if args.command == 'advance':
                store.advance(args.watermark_ms)
            result = store.report()
        print(json.dumps(result, indent=2, allow_nan=False))
    except (OSError, ValueError, sqlite3.Error) as error:
        parser.exit(2, f'windowflow: {error}\n')
    finally:
        if store is not None:
            store.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
