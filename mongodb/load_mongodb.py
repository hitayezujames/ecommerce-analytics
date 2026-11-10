"""
load_mongodb.py - Part 1: load the catalogue, users and transactions into MongoDB.

Document model (see report section 2.1 for the full justification):

  categories   {_id: cat_id, name, subcategories:[{subcategory_id, name, profit_margin}]}
               -> small, read-mostly hierarchy; subcategories EMBEDDED (bounded 3-5).

  products     {_id: prod_id, name, base_price, current_stock, is_active,
                category:{category_id, name},                         <- extended reference
                subcategory:{subcategory_id, name, profit_margin},    <- extended reference
                price_history:[{price, date}], creation_date}
               -> category data copied in so catalogue pages / filters need no $lookup.
                  price_history is bounded (1-3 entries) so it is embedded.

  transactions {_id: txn_id, user_id, session_id, timestamp(Date), status, payment_method,
                subtotal, discount, total,
                items:[{product_id, product_name, category_id, subcategory_id,
                        profit_margin, quantity, unit_price, subtotal}]}
               -> line items EMBEDDED (always read with the order, bounded size, written once).
                  Product/category fields are a snapshot at time of sale, so revenue-by-category
                  aggregations run on one collection without joins.

  users        {_id: user_id, geo_data, registration_date, last_active,
                purchase_summary:{total_orders, total_spent, avg_order_value,
                                  first_purchase, last_purchase, favourite_category,
                                  recent_transactions:[last 5 txn ids]}}
               -> "computed + subset pattern": a profile page needs the summary, not
                  every order. Built server-side with $merge after transactions load.

Sessions are deliberately NOT stored here - the high-volume clickstream lives in HBase.

Usage:  python load_mongodb.py [--limit-transactions N] [--keep]
"""
import argparse
import datetime as dt
import json
import os
import sys
import time

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import BulkWriteError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config  # noqa: E402

BATCH = 5000


def to_date(s):
    """ISO-8601 string -> datetime (stored as BSON Date, enabling date operators)."""
    return dt.datetime.fromisoformat(s) if s else None


def load_json(name):
    path = os.path.join(config.RAW_DIR, name)
    with open(path) as f:
        return json.load(f)


def insert_batched(coll, docs):
    """Unordered bulk inserts: one bad document does not abort the batch."""
    inserted = 0
    for i in range(0, len(docs), BATCH):
        try:
            res = coll.insert_many(docs[i:i + BATCH], ordered=False)
            inserted += len(res.inserted_ids)
        except BulkWriteError as e:
            inserted += e.details.get("nInserted", 0)
            print(f"  ! {len(e.details.get('writeErrors', []))} documents rejected in batch {i // BATCH}")
    return inserted


def create_validators(db):
    """$jsonSchema validation guards data quality at write time."""
    db.create_collection("transactions", validator={"$jsonSchema": {
        "bsonType": "object",
        "required": ["user_id", "timestamp", "items", "total"],
        "properties": {
            "user_id": {"bsonType": "string", "pattern": "^user_"},
            "timestamp": {"bsonType": "date"},
            "total": {"bsonType": ["double", "int"], "minimum": 0},
            "items": {"bsonType": "array", "minItems": 1, "items": {
                "bsonType": "object",
                "required": ["product_id", "quantity", "unit_price"],
                "properties": {"quantity": {"bsonType": "int", "minimum": 1}}}},
        }}}, validationLevel="moderate")
    db.create_collection("products", validator={"$jsonSchema": {
        "bsonType": "object",
        "required": ["name", "base_price", "category"],
        "properties": {"base_price": {"bsonType": ["double", "int"], "minimum": 0},
                       "current_stock": {"bsonType": "int", "minimum": 0}}}},
        validationLevel="moderate")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-transactions", type=int, default=0, help="0 = load all")
    ap.add_argument("--keep", action="store_true", help="do not drop existing collections")
    args = ap.parse_args()

    client = MongoClient(config.MONGO_URI)
    db = client[config.MONGO_DB]
    t0 = time.time()

    if not args.keep:
        for c in ("categories", "products", "users", "transactions"):
            db.drop_collection(c)
        create_validators(db)

    # ---- categories ---------------------------------------------------------
    categories = load_json("categories.json")
    cat_by_id = {c["category_id"]: c for c in categories}
    sub_by_id = {s["subcategory_id"]: s for c in categories for s in c["subcategories"]}
    db.categories.insert_many([dict(c, _id=c["category_id"]) for c in categories])
    print(f"categories: {len(categories)}")

    # ---- products (with denormalised category / subcategory) ----------------
    products = load_json("products.json")
    prod_by_id = {}
    docs = []
    for p in products:
        cat = cat_by_id.get(p["category_id"], {})
        sub = sub_by_id.get(p.get("subcategory_id"), {})
        doc = {
            "_id": p["product_id"],
            "name": p["name"].strip(),
            "base_price": float(p["base_price"]),
            "current_stock": int(p["current_stock"]),
            "is_active": bool(p["is_active"]),
            "category": {"category_id": p["category_id"], "name": cat.get("name")},
            "subcategory": {"subcategory_id": sub.get("subcategory_id"),
                            "name": sub.get("name"),
                            "profit_margin": sub.get("profit_margin")},
            "price_history": [{"price": float(h["price"]), "date": to_date(h["date"])}
                              for h in p["price_history"]],
            "creation_date": to_date(p["creation_date"]),
        }
        docs.append(doc)
        prod_by_id[p["product_id"]] = doc
    print(f"products: {insert_batched(db.products, docs)}")

    # ---- users ----------------------------------------------------------------
    users = load_json("users.json")
    docs = [{"_id": u["user_id"],
             "geo_data": {k: (v.strip() if isinstance(v, str) else v) for k, v in u["geo_data"].items()},
             "registration_date": to_date(u["registration_date"]),
             "last_active": to_date(u["last_active"])} for u in users]
    print(f"users: {insert_batched(db.users, docs)}")

    # ---- transactions (items enriched with product snapshot) ------------------
    transactions = load_json("transactions.json")
    if args.limit_transactions:
        transactions = transactions[:args.limit_transactions]
    docs = []
    for t in transactions:
        items = []
        for it in t["items"]:
            p = prod_by_id.get(it["product_id"], {})
            items.append({
                "product_id": it["product_id"],
                "product_name": p.get("name"),
                "category_id": p.get("category", {}).get("category_id"),
                "subcategory_id": p.get("subcategory", {}).get("subcategory_id"),
                "profit_margin": p.get("subcategory", {}).get("profit_margin"),
                "quantity": int(it["quantity"]),
                "unit_price": float(it["unit_price"]),
                "subtotal": float(it["subtotal"]),
            })
        docs.append({
            "_id": t["transaction_id"],
            "user_id": t["user_id"],
            "session_id": t.get("session_id"),
            "timestamp": to_date(t["timestamp"]),
            "status": t["status"].lower(),
            "payment_method": t["payment_method"].lower(),
            "subtotal": float(t["subtotal"]),
            "discount": float(t["discount"]),
            "total": float(t["total"]),
            "items": items,
        })
    print(f"transactions: {insert_batched(db.transactions, docs)}")

    # ---- indexes (chosen from the query patterns in aggregations.py / integration) ---
    db.transactions.create_index([("user_id", ASCENDING), ("timestamp", DESCENDING)])  # a user's orders, newest first
    db.transactions.create_index([("timestamp", ASCENDING)])                           # time-range revenue
    db.transactions.create_index([("items.product_id", ASCENDING)])                    # multikey: orders containing X
    db.transactions.create_index([("session_id", ASCENDING)],
                                 partialFilterExpression={"session_id": {"$type": "string"}})  # funnel join
    db.products.create_index([("category.category_id", ASCENDING), ("base_price", ASCENDING)])
    db.products.create_index([("subcategory.subcategory_id", ASCENDING)])
    db.users.create_index([("geo_data.country", ASCENDING)])
    db.users.create_index([("purchase_summary.total_spent", DESCENDING)])

    # ---- users.purchase_summary computed in the database with $merge ------------
    db.transactions.aggregate([
        {"$sort": {"timestamp": 1}},
        {"$group": {
            "_id": "$user_id",
            "total_orders": {"$sum": 1},
            "total_spent": {"$sum": "$total"},
            "first_purchase": {"$first": "$timestamp"},
            "last_purchase": {"$last": "$timestamp"},
            "recent_transactions": {"$lastN": {"n": 5, "input": "$_id"}},
        }},
        {"$project": {"purchase_summary": {
            "total_orders": "$total_orders",
            "total_spent": {"$round": ["$total_spent", 2]},
            "avg_order_value": {"$round": [{"$divide": ["$total_spent", "$total_orders"]}, 2]},
            "first_purchase": "$first_purchase",
            "last_purchase": "$last_purchase",
            "recent_transactions": "$recent_transactions"}}},
        {"$merge": {"into": "users", "on": "_id", "whenMatched": "merge", "whenNotMatched": "discard"}},
    ], allowDiskUse=True)

    # favourite category = category with the highest spend for each user
    db.transactions.aggregate([
        {"$unwind": "$items"},
        {"$group": {"_id": {"u": "$user_id", "c": "$items.category_id"},
                    "spend": {"$sum": "$items.subtotal"}}},
        {"$sort": {"spend": -1}},
        {"$group": {"_id": "$_id.u", "fav": {"$first": "$_id.c"}}},
        # pipeline-style whenMatched so we set ONE nested field instead of replacing purchase_summary
        {"$merge": {"into": "users", "on": "_id",
                    "whenMatched": [{"$set": {"purchase_summary.favourite_category": "$$new.fav"}}],
                    "whenNotMatched": "discard"}},
    ], allowDiskUse=True)

    # users who never bought get an explicit empty summary (avoids null checks downstream)
    db.users.update_many({"purchase_summary": {"$exists": False}},
                         {"$set": {"purchase_summary": {"total_orders": 0, "total_spent": 0.0,
                                                        "avg_order_value": 0.0, "recent_transactions": []}}})

    print(f"Done in {time.time() - t0:.1f}s")
    print("Sample user:", db.users.find_one({"purchase_summary.total_orders": {"$gte": 3}}))


if __name__ == "__main__":
    main()
