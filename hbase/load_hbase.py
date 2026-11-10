"""
load_hbase.py - Part 1: load session clickstream + daily product metrics into HBase.

  user_sessions          row = user_id|reverse_ts|session_id
                         s:*  session metadata, pv:NNN page views (JSON), c:<product_id> cart lines
  product_metrics_daily  row = product_id|yyyymmdd
                         m:views, m:cart_adds, m:purchases, m:units_sold, m:revenue_cents (8-byte counters)

Usage:  python load_hbase.py [--files N]     (N session files of 100k sessions; default from config)
Prereq: tables created with create_schema.hbase and the Thrift server listening on 9090.
"""
import argparse
import glob
import json
import os
import time
from collections import defaultdict

from hbase_utils import config, connect, metric_row_key, pack_long, session_row_key

BATCH_SIZE = int(os.environ.get("HBASE_BATCH_SIZE", "200"))
PAUSE_MS = int(os.environ.get("HBASE_PAUSE_MS", "0"))   # e.g. 200 on a 4 GB laptop  # small batches: Thrift has a max message size


class RobustWriter:
    """Sends puts in small batches; if the Thrift connection drops (Broken pipe / timeout),
    it reconnects and re-sends the same batch (puts are idempotent, so retrying is safe)."""

    def __init__(self, table_name, batch_size=BATCH_SIZE, retries=int(os.environ.get("HBASE_RETRIES", "8")),
                 offset=0):
        self.table_name, self.batch_size, self.retries = table_name, batch_size, retries
        self.buf, self.sent, self.offset = [], 0, offset
        self._open()

    def _open(self):
        self.conn = connect(retries=20)
        self.table = self.conn.table(self.table_name)

    def put(self, key, cells):
        self.buf.append((key, cells))
        if len(self.buf) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.buf:
            return
        for attempt in range(1, self.retries + 1):
            try:
                with self.table.batch() as b:
                    for key, cells in self.buf:
                        b.put(key, cells)
                self.sent += len(self.buf)
                self.buf = []
                if PAUSE_MS:
                    time.sleep(PAUSE_MS / 1000)   # give a small HBase time to flush / garbage-collect
                return
            except Exception as exc:  # noqa: BLE001  (BrokenPipe, TTransportException, timeouts)
                print(f"  ! write failed ({type(exc).__name__}), reconnecting (attempt {attempt}/{self.retries})")
                try:
                    self.conn.close()
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(3 * attempt)
                self._open()
        done = self.offset + self.sent
        raise SystemExit(f"\nHBase keeps refusing writes. {done:,} rows were saved safely.\n"
                         f"Restart HBase (docker restart hbase), wait 2 minutes, then CONTINUE with:\n"
                         f"    python load_hbase.py --files 0 --skip {done}\n")

    def close(self):
        self.flush()
        self.conn.close()


def session_cells(s):
    """Flatten one session document into {b'cf:qualifier': b'value'} cells."""
    geo, dev = s.get("geo_data") or {}, s.get("device_profile") or {}
    cells = {
        b"s:session_id": s["session_id"],
        b"s:start_time": s["start_time"],
        b"s:end_time": s["end_time"],
        b"s:duration": str(s["duration_seconds"]),
        b"s:status": s["conversion_status"],
        b"s:referrer": s["referrer"],
        b"s:device": dev.get("type", ""),
        b"s:os": dev.get("os", ""),
        b"s:browser": dev.get("browser", ""),
        b"s:country": geo.get("country", ""),
        b"s:city": geo.get("city", ""),
        b"s:ip": geo.get("ip_address", ""),
        b"s:n_pages": str(len(s["page_views"])),
        b"s:n_products": str(len(s["viewed_products"])),
    }
    # one column per page view: sparse + ordered by sequence number
    for i, pv in enumerate(s["page_views"]):
        cells[f"pv:{i:03d}".encode()] = json.dumps(
            {"ts": pv["timestamp"], "type": pv["page_type"], "pid": pv["product_id"],
             "cid": pv["category_id"], "dur": pv["view_duration"]}, separators=(",", ":"))
    # one column per cart line: qualifier is the product id
    for pid, line in (s.get("cart_contents") or {}).items():
        cells[f"c:{pid}".encode()] = f"{line['quantity']}|{line['price']}"
    return {k: (v.encode() if isinstance(v, str) else v) for k, v in cells.items()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", type=int, default=config.HBASE_SESSION_FILES, help="0 = all session files")
    ap.add_argument("--max-sessions", type=int, default=int(os.environ.get("HBASE_MAX_SESSIONS", "0")),
                    help="stop after N sessions (0 = no limit); a representative subset for small machines")
    ap.add_argument("--skip", type=int, default=0,
                    help="resume: do not re-write the first N sessions (they are still counted in the metrics)")
    args = ap.parse_args()

    files = sorted(glob.glob(os.path.join(config.RAW_DIR, "sessions_*.json")),
                   key=lambda p: int(p.rsplit("_", 1)[1].split(".")[0]))
    if args.files:
        files = files[:args.files]

    writer = RobustWriter(config.HBASE_SESSIONS_TABLE, offset=args.skip)
    metrics = defaultdict(lambda: defaultdict(int))  # (product, day) -> counter -> value
    loaded_sessions = set()
    t0, n = time.time(), 0

    for path in files:
        with open(path) as f:
            sessions = json.load(f)
        for s in sessions:
            if args.max_sessions and n >= args.max_sessions:
                break
            if not s.get("user_id") or not s.get("start_time"):
                continue  # skip malformed rows rather than failing the whole load
            if n >= args.skip:   # sessions before --skip were written by an earlier run
                writer.put(session_row_key(s["user_id"], s["start_time"], s["session_id"]), session_cells(s))
            loaded_sessions.add(s["session_id"])
            day = s["start_time"][:10]
            for pv in s["page_views"]:
                if pv["page_type"] == "product_detail" and pv["product_id"]:
                    metrics[(pv["product_id"], day)]["views"] += 1
            for pid in (s.get("cart_contents") or {}):
                metrics[(pid, day)]["cart_adds"] += 1
            n += 1
            if n % 10000 == 0 and n > args.skip:
                print(f"  {n:,} sessions written ({time.time() - t0:.0f}s)", flush=True)
        writer.flush()
        del sessions
        if args.max_sessions and n >= args.max_sessions:
            print(f"  reached --max-sessions={args.max_sessions:,} (representative subset)")
            break
        print(f"{os.path.basename(path)}: {n:,} sessions loaded ({time.time() - t0:.0f}s)")

    # purchases come from transactions (all of them - online & offline sales)
    with open(os.path.join(config.RAW_DIR, "transactions.json")) as f:
        for t in json.load(f):
            day = t["timestamp"][:10]
            for it in t["items"]:
                m = metrics[(it["product_id"], day)]
                m["purchases"] += 1
                m["units_sold"] += it["quantity"]
                m["revenue_cents"] += round(it["subtotal"] * 100)

    writer.close()
    mwriter = RobustWriter(config.HBASE_PRODUCT_METRICS_TABLE, batch_size=BATCH_SIZE * 5)  # tiny rows
    for (pid, day), counters in metrics.items():
        mwriter.put(metric_row_key(pid, day), {f"m:{k}".encode(): pack_long(v) for k, v in counters.items()})
    mwriter.close()
    print(f"product_metrics_daily: {len(metrics):,} rows. Total time {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
