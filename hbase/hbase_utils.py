"""Shared helpers for the HBase row-key design (used by loader, queries and integration)."""
import datetime as dt
import os
import struct
import sys
import time

import happybase

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config  # noqa: E402

MAX_TS = 9_999_999_999  # reverse timestamps are MAX_TS - epoch_seconds (10 digits)
SEP = "|"


def epoch(iso):
    """Naive ISO timestamps are treated as UTC everywhere for consistent keys."""
    return int(dt.datetime.fromisoformat(iso).replace(tzinfo=dt.timezone.utc).timestamp())


def reverse_ts(iso):
    return f"{MAX_TS - epoch(iso):010d}"


def session_row_key(user_id, start_time, session_id):
    # user first -> locality per user; reverse ts -> newest sessions first in a scan;
    # session_id suffix -> uniqueness when two sessions start in the same second
    return f"{user_id}{SEP}{reverse_ts(start_time)}{SEP}{session_id}".encode()


def user_time_range(user_id, start_iso, end_iso):
    """[row_start, row_stop) covering sessions that started in [start, end).
    Reverse timestamps flip the order: the NEWER bound becomes the start row."""
    row_start = f"{user_id}{SEP}{MAX_TS - epoch(end_iso) + 1:010d}".encode()
    row_stop = f"{user_id}{SEP}{MAX_TS - epoch(start_iso) + 1:010d}".encode()
    return row_start, row_stop


def metric_row_key(product_id, day):
    return f"{product_id}{SEP}{day.replace('-', '')}".encode()  # day = yyyy-mm-dd


def pack_long(n):
    """HBase counters are 8-byte big-endian signed longs."""
    return struct.pack(">q", int(n))


def unpack_long(b):
    return struct.unpack(">q", b)[0] if b else 0


def connect(retries=5):
    """Thrift connections are flaky while HBase starts - retry with back-off."""
    for attempt in range(1, retries + 1):
        try:
            conn = happybase.Connection(config.HBASE_HOST, port=config.HBASE_PORT, timeout=60000)
            conn.tables()  # forces the connection open
            return conn
        except Exception as exc:  # noqa: BLE001
            print(f"HBase not reachable (attempt {attempt}/{retries}): {exc}")
            time.sleep(2 * attempt)
    raise SystemExit("Could not connect to the HBase Thrift server - is the container running?")
