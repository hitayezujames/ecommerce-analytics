"""
aggregations.py - Part 1: analytical aggregation pipelines on MongoDB.

  1. revenue_by_category_month : net revenue + gross profit per category per month
                                 (order-level discount allocated pro-rata to line items)
  2. product_popularity        : $facet -> top products by units, by revenue, by #buyers
  3. customer_segmentation     : RFM-style segments from purchase frequency, spend, recency

Each pipeline is printed, executed, written to data/output/mongo_<name>.csv and its
winning index plan is shown with explain() so the index design can be checked.

Usage:  python aggregations.py [--explain]
"""
import argparse
import datetime as dt
import json
import os
import sys

import pandas as pd
from pymongo import MongoClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config  # noqa: E402

REF_DATE = dt.datetime.fromisoformat(config.REFERENCE_DATE)


# ---------------------------------------------------------------------------
# 1. Revenue analytics: revenue & gross profit by category and month
# ---------------------------------------------------------------------------
def revenue_by_category_month(start=None, end=None):
    match = {"status": {"$in": ["completed", "shipped", "delivered", "processing"]}}
    if start or end:
        match["timestamp"] = {k: v for k, v in (("$gte", start), ("$lt", end)) if v}
    return [
        {"$match": match},                                    # uses timestamp index when a range is given
        {"$project": {
            "month": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}},
            # share of the order kept after discount, applied to every line item
            "net_factor": {"$cond": [{"$gt": ["$subtotal", 0]},
                                     {"$divide": ["$total", "$subtotal"]}, 1]},
            "items": 1}},
        {"$unwind": "$items"},
        {"$group": {
            "_id": {"category_id": "$items.category_id", "month": "$month"},
            "units": {"$sum": "$items.quantity"},
            "net_revenue": {"$sum": {"$multiply": ["$items.subtotal", "$net_factor"]}},
            "gross_profit": {"$sum": {"$multiply": ["$items.subtotal", "$net_factor",
                                                    {"$ifNull": ["$items.profit_margin", 0]}]}},
            "orders": {"$addToSet": "$_id"}}},
        {"$lookup": {"from": "categories", "localField": "_id.category_id",   # 25 docs: cheap join
                     "foreignField": "_id", "as": "cat"}},
        {"$project": {
            "_id": 0,
            "category_id": "$_id.category_id",
            "category_name": {"$first": "$cat.name"},
            "month": "$_id.month",
            "units": 1,
            "orders": {"$size": "$orders"},
            "net_revenue": {"$round": ["$net_revenue", 2]},
            "gross_profit": {"$round": ["$gross_profit", 2]},
            "margin_pct": {"$round": [{"$multiply": [{"$divide": ["$gross_profit", "$net_revenue"]}, 100]}, 1]}}},
        {"$sort": {"month": 1, "net_revenue": -1}},
    ]


# ---------------------------------------------------------------------------
# 2. Product popularity: several rankings in ONE pass with $facet
# ---------------------------------------------------------------------------
def product_popularity(top_n=10):
    return [
        {"$unwind": "$items"},
        {"$group": {
            "_id": "$items.product_id",
            "product_name": {"$first": "$items.product_name"},
            "category_id": {"$first": "$items.category_id"},
            "units_sold": {"$sum": "$items.quantity"},
            "revenue": {"$sum": "$items.subtotal"},
            "buyers": {"$addToSet": "$user_id"},
            "orders": {"$sum": 1}}},
        {"$addFields": {"distinct_buyers": {"$size": "$buyers"}, "revenue": {"$round": ["$revenue", 2]}}},
        {"$project": {"buyers": 0}},
        {"$facet": {
            "top_by_units": [{"$sort": {"units_sold": -1, "_id": 1}}, {"$limit": top_n}],
            "top_by_revenue": [{"$sort": {"revenue": -1, "_id": 1}}, {"$limit": top_n}],
            "top_by_buyers": [{"$sort": {"distinct_buyers": -1, "_id": 1}}, {"$limit": top_n}],
            "catalogue_coverage": [{"$count": "products_with_sales"}]}},
    ]


# ---------------------------------------------------------------------------
# 3. Customer segmentation (RFM-lite): frequency x monetary x recency
# ---------------------------------------------------------------------------
def customer_segmentation():
    return [
        {"$group": {"_id": "$user_id",
                    "orders": {"$sum": 1},
                    "spent": {"$sum": "$total"},
                    "last_purchase": {"$max": "$timestamp"}}},
        {"$addFields": {"recency_days": {"$dateDiff": {"startDate": "$last_purchase",
                                                       "endDate": REF_DATE, "unit": "day"}}}},
        {"$addFields": {"segment": {"$switch": {"branches": [
            {"case": {"$and": [{"$gte": ["$orders", 8]}, {"$lte": ["$recency_days", 14]}]}, "then": "1 Champions"},
            {"case": {"$and": [{"$gte": ["$orders", 5]}, {"$lte": ["$recency_days", 30]}]}, "then": "2 Loyal"},
            {"case": {"$and": [{"$gte": ["$orders", 5]}, {"$gt": ["$recency_days", 30]}]}, "then": "3 At risk (was loyal)"},
            {"case": {"$and": [{"$gte": ["$orders", 2]}, {"$lte": ["$recency_days", 30]}]}, "then": "4 Developing"},
            {"case": {"$gte": ["$orders", 2]}, "then": "5 Lapsing"},
            {"case": {"$lte": ["$recency_days", 30]}, "then": "6 New / one-time recent"}],
            "default": "7 One-time, inactive"}}}},
        {"$group": {"_id": "$segment",
                    "customers": {"$sum": 1},
                    "avg_orders": {"$avg": "$orders"},
                    "avg_spent": {"$avg": "$spent"},
                    "total_revenue": {"$sum": "$spent"},
                    "avg_recency_days": {"$avg": "$recency_days"}}},
        {"$setWindowFields": {"sortBy": {"_id": 1},              # share of revenue per segment
                              "output": {"all_revenue": {"$sum": "$total_revenue",
                                                         "window": {"documents": ["unbounded", "unbounded"]}}}}},
        {"$project": {"_id": 0, "segment": "$_id", "customers": 1,
                      "avg_orders": {"$round": ["$avg_orders", 2]},
                      "avg_spent": {"$round": ["$avg_spent", 2]},
                      "total_revenue": {"$round": ["$total_revenue", 2]},
                      "revenue_share_pct": {"$round": [{"$multiply": [{"$divide": ["$total_revenue", "$all_revenue"]}, 100]}, 1]},
                      "avg_recency_days": {"$round": ["$avg_recency_days", 1]}}},
        {"$sort": {"segment": 1}},
    ]


# Frequency distribution used for the histogram in the report
def order_frequency_buckets():
    return [
        {"$group": {"_id": "$user_id", "orders": {"$sum": 1}, "spent": {"$sum": "$total"}}},
        {"$bucket": {"groupBy": "$orders", "boundaries": [1, 2, 3, 5, 8, 12, 1000],
                     "default": "other",
                     "output": {"customers": {"$sum": 1}, "avg_spent": {"$avg": "$spent"}}}},
    ]


def run(db, name, pipeline, explain=False):
    print(f"\n=== {name} ===")
    print(json.dumps(pipeline, default=str, indent=1)[:1500])
    if explain:
        plan = db.command("explain", {"aggregate": "transactions", "pipeline": pipeline, "cursor": {}},
                          verbosity="queryPlanner")
        stages = plan.get("stages", [plan])
        winning = stages[0].get("$cursor", plan).get("queryPlanner", {}).get("winningPlan", {})
        print("winning plan:", json.dumps(winning, default=str)[:400])
    return list(db.transactions.aggregate(pipeline, allowDiskUse=True))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--explain", action="store_true")
    args = ap.parse_args()
    db = MongoClient(config.MONGO_URI)[config.MONGO_DB]

    rows = run(db, "Revenue by category and month", revenue_by_category_month(), args.explain)
    pd.DataFrame(rows).to_csv(os.path.join(config.OUTPUT_DIR, "mongo_revenue_by_category_month.csv"), index=False)
    print(pd.DataFrame(rows).head(10).to_string())

    # same pipeline restricted to the last 30 days -> exercises the timestamp index
    last30 = run(db, "Revenue by category - last 30 days",
                 revenue_by_category_month(REF_DATE - dt.timedelta(days=30), REF_DATE), args.explain)
    print(pd.DataFrame(last30).groupby("category_name")["net_revenue"].sum().nlargest(5))

    facets = run(db, "Product popularity", product_popularity(), args.explain)[0]
    for key in ("top_by_units", "top_by_revenue", "top_by_buyers"):
        df = pd.DataFrame(facets[key]).rename(columns={"_id": "product_id"})
        df.to_csv(os.path.join(config.OUTPUT_DIR, f"mongo_{key}.csv"), index=False)
        print(f"\n{key}:\n{df.head(5).to_string()}")
    print("coverage:", facets["catalogue_coverage"])

    seg = pd.DataFrame(run(db, "Customer segmentation (RFM-lite)", customer_segmentation(), args.explain))
    seg.to_csv(os.path.join(config.OUTPUT_DIR, "mongo_customer_segments.csv"), index=False)
    print(seg.to_string())

    buckets = pd.DataFrame(run(db, "Order-frequency buckets", order_frequency_buckets()))
    buckets.to_csv(os.path.join(config.OUTPUT_DIR, "mongo_order_frequency_buckets.csv"), index=False)
    print(buckets.to_string())


if __name__ == "__main__":
    main()
