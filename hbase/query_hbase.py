"""
query_hbase.py - Part 1: retrieval patterns the row-key design was built for.

  Q1  latest N sessions of a user            -> prefix scan, metadata CF only, LIMIT N
  Q2  a user's sessions in a time window     -> bounded range scan (reverse timestamps)
  Q3  full journey of one session            -> single-row GET of the 'pv' family
  Q4  product daily metrics for a date range -> range scan on product_id|yyyymmdd
  Q5  view-to-purchase conversion by week    -> client-side roll-up of Q4
  Q6  real-time counter update               -> atomic INCREMENT (what a live stream would do)

Usage:  python query_hbase.py --user user_000042 --product prod_00123
"""
import argparse
import json
from collections import defaultdict
from datetime import date

from hbase_utils import config, connect, metric_row_key, unpack_long, user_time_range


def q1_latest_sessions(tbl, user_id, n=5):
    rows = tbl.scan(row_prefix=f"{user_id}|".encode(), columns=[b"s"], limit=n)
    return [{k.decode().split(":")[1]: v.decode() for k, v in data.items()} for _, data in rows]


def q2_sessions_in_window(tbl, user_id, start_iso, end_iso):
    row_start, row_stop = user_time_range(user_id, start_iso, end_iso)
    return [(key.decode(), data[b"s:start_time"].decode(), data[b"s:status"].decode())
            for key, data in tbl.scan(row_start=row_start, row_stop=row_stop,
                                      columns=[b"s:start_time", b"s:status"])]


def q3_session_journey(tbl, row_key):
    row = tbl.row(row_key, columns=[b"pv", b"c"])
    pages = [json.loads(v) for k, v in sorted(row.items()) if k.startswith(b"pv:")]
    cart = {k.decode()[2:]: v.decode() for k, v in row.items() if k.startswith(b"c:")}
    return pages, cart


def q4_product_metrics(tbl, product_id, start_day, end_day):
    out = []
    for key, data in tbl.scan(row_start=metric_row_key(product_id, start_day),
                              row_stop=metric_row_key(product_id, end_day) + b"~"):  # inclusive end
        rec = {c.decode()[2:]: unpack_long(v) for c, v in data.items()}
        rec["day"] = key.decode().split("|")[1]
        out.append(rec)
    return out


def q5_weekly_conversion(daily):
    weeks = defaultdict(lambda: defaultdict(int))
    for r in daily:
        d = date(int(r["day"][:4]), int(r["day"][4:6]), int(r["day"][6:]))
        wk = f"{d.isocalendar()[0]}-W{d.isocalendar()[1]:02d}"
        for k in ("views", "cart_adds", "purchases", "units_sold"):
            weeks[wk][k] += r.get(k, 0)
    for wk, m in sorted(weeks.items()):
        rate = m["purchases"] / m["views"] if m["views"] else 0
        print(f"  {wk}: views={m['views']:3d} cart_adds={m['cart_adds']:3d} "
              f"purchases={m['purchases']:3d} view->purchase={rate:.1%}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--user", default="user_000042")
    ap.add_argument("--product", default="prod_00123")
    ap.add_argument("--start", default="2026-08-01")
    ap.add_argument("--end", default="2026-09-01")
    args = ap.parse_args()

    conn = connect()
    sessions = conn.table(config.HBASE_SESSIONS_TABLE)
    metrics = conn.table(config.HBASE_PRODUCT_METRICS_TABLE)

    print(f"\nQ1 - latest 5 sessions of {args.user}")
    latest = q1_latest_sessions(sessions, args.user)
    for s in latest:
        print(f"  {s['start_time']}  {s['device']:<7} {s['referrer']:<13} pages={s['n_pages']:>2} {s['status']}")

    print(f"\nQ2 - sessions of {args.user} between {args.start} and {args.end}")
    for key, start, status in q2_sessions_in_window(sessions, args.user, args.start, args.end):
        print(f"  {start}  {status:<10} {key}")

    if latest:
        first_key = next(iter(sessions.scan(row_prefix=f"{args.user}|".encode(), limit=1)))[0]
        print(f"\nQ3 - journey of {first_key.decode()}")
        pages, cart = q3_session_journey(sessions, first_key)
        for p in pages:
            print(f"  {p['ts']}  {p['type']:<17} {p['pid'] or '':<11} {p['dur']}s")
        print("  cart:", cart)

    print(f"\nQ4/Q5 - {args.product} metrics, weekly roll-up")
    daily = q4_product_metrics(metrics, args.product, "2026-07-01", "2026-10-01")
    print(f"  {len(daily)} daily rows")
    q5_weekly_conversion(daily)

    print("\nQ6 - atomic increment (simulated live page view)")
    key = metric_row_key(args.product, "2026-10-01")
    before = metrics.counter_get(key, b"m:views")
    after = metrics.counter_inc(key, b"m:views", value=1)
    metrics.counter_dec(key, b"m:views", value=1)  # undo, keep the dataset unchanged
    print(f"  views {before} -> {after} (reverted)")
    conn.close()


if __name__ == "__main__":
    main()
