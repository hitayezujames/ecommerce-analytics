"""
sources.py - one interface, two back-ends, for the integrated analytics (Part 3).

  --source files : read the cleaned Parquet produced by spark/01_clean_normalize.py
  --source db    : read the operational stores
                     MongoDB  -> users, transactions (+ embedded order lines)
                     HBase    -> user_sessions (metadata + page-view funnel flags)

Both back-ends return Spark DataFrames with IDENTICAL column names, so the analytics
code (clv_analysis.py, funnel_analysis.py) and the Spark SQL in 04_spark_sql_analytics.py
run unchanged whichever store the data came from.

Projection push-down: only the fields the analysis needs are requested from MongoDB
(projection) and HBase (column-family / qualifier selection) - the clickstream page-view
family is only scanned when funnel flags are needed.

For production volumes swap the pymongo path for the MongoDB Spark Connector
(spark.read.format("mongodb")) and the happybase scan for the hbase-spark connector so
reads are parallelised across executors; set USE_MONGO_SPARK_CONNECTOR=1 to try it.
"""
import datetime as dt
import json
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, IntegerType, LongType, StringType, StructField,
                               StructType, TimestampType)

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "spark"))
sys.path.insert(0, os.path.join(HERE, "..", "hbase"))
from common import config, get_spark, read_processed  # noqa: E402,F401

SESSION_COLS = ["session_id", "user_id", "start_ts", "session_date", "duration_seconds", "device_type",
                "referrer", "conversion_status", "n_pages", "viewed_product", "reached_cart",
                "reached_checkout", "cart_units"]


# --------------------------------------------------------------------------- files
def _files(spark, with_pages):
    users = read_processed(spark, "dim_users").select("user_id", "country", "registration_ts")
    tx = read_processed(spark, "fact_transactions").select("transaction_id", "user_id", "session_id",
                                                            "txn_ts", "total")
    lines = read_processed(spark, "fact_transaction_items").select("transaction_id", "user_id", "category_id",
                                                                   "net_revenue", "gross_profit")
    sessions = read_processed(spark, "fact_sessions").select(*SESSION_COLS)
    return users, tx, lines, sessions


# ------------------------------------------------------------------------- MongoDB
def _mongo(spark):
    if os.environ.get("USE_MONGO_SPARK_CONNECTOR") == "1":
        # requires --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0
        rd = lambda c: (spark.read.format("mongodb").option("connection.uri", config.MONGO_URI)  # noqa: E731
                        .option("database", config.MONGO_DB).option("collection", c).load())
        users = rd("users").select(F.col("_id").alias("user_id"), F.col("geo_data.country").alias("country"),
                                   F.col("registration_date").alias("registration_ts"))
        raw = rd("transactions")
    else:
        from pymongo import MongoClient
        db = MongoClient(config.MONGO_URI)[config.MONGO_DB]
        u_rows = [(d["_id"], (d.get("geo_data") or {}).get("country"), d.get("registration_date"))
                  for d in db.users.find({}, {"geo_data.country": 1, "registration_date": 1})]
        users = spark.createDataFrame(u_rows, StructType([
            StructField("user_id", StringType()), StructField("country", StringType()),
            StructField("registration_ts", TimestampType())]))
        t_rows = [(d["_id"], d["user_id"], d.get("session_id"), d["timestamp"], float(d["subtotal"]),
                   float(d["total"]),
                   [(i.get("category_id"), float(i["subtotal"]), float(i.get("profit_margin") or 0.0))
                    for i in d["items"]])
                  for d in db.transactions.find({}, {"user_id": 1, "session_id": 1, "timestamp": 1, "subtotal": 1,
                                                     "total": 1, "items.category_id": 1, "items.subtotal": 1,
                                                     "items.profit_margin": 1}, batch_size=10000)]
        raw = spark.createDataFrame(t_rows, "_id string, user_id string, session_id string, timestamp timestamp, "
                                            "subtotal double, total double, "
                                            "items array<struct<category_id:string,subtotal:double,profit_margin:double>>")
    tx = raw.select(F.col("_id").alias("transaction_id"), "user_id", "session_id",
                    F.col("timestamp").alias("txn_ts"), "total")
    lines = (raw.select(F.col("_id").alias("transaction_id"), "user_id",
                        (F.col("total") / F.col("subtotal")).alias("nf"), F.explode("items").alias("i"))
             .select("transaction_id", "user_id", F.col("i.category_id").alias("category_id"),
                     F.round(F.col("i.subtotal") * F.col("nf"), 2).alias("net_revenue"),
                     F.round(F.col("i.subtotal") * F.col("nf") * F.col("i.profit_margin"), 2).alias("gross_profit")))
    return users, tx, lines


# --------------------------------------------------------------------------- HBase
def _hbase(spark, with_pages):
    from hbase_utils import connect
    conn = connect()
    table = conn.table(config.HBASE_SESSIONS_TABLE)
    columns = [b"s", b"c"] + ([b"pv"] if with_pages else [])
    rows = []
    for key, d in table.scan(columns=columns, batch_size=500):
        g = lambda q: d.get(b"s:" + q, b"").decode()  # noqa: E731
        types = {json.loads(v)["type"] for k, v in d.items() if k.startswith(b"pv:")} if with_pages else set()
        start = dt.datetime.fromisoformat(g(b"start_time"))
        rows.append((g(b"session_id"), key.decode().split("|")[0], start, start.date(), int(g(b"duration")),
                     g(b"device"), g(b"referrer"), g(b"status"), int(g(b"n_pages") or 0),
                     int("product_detail" in types or int(g(b"n_products") or 0) > 0),
                     int("cart" in types), int("checkout" in types),
                     sum(int(v.decode().split("|")[0]) for k, v in d.items() if k.startswith(b"c:"))))
    conn.close()
    schema = StructType([StructField("session_id", StringType()), StructField("user_id", StringType()),
                         StructField("start_ts", TimestampType()), StructField("session_date", DateType()),
                         StructField("duration_seconds", LongType()), StructField("device_type", StringType()),
                         StructField("referrer", StringType()), StructField("conversion_status", StringType()),
                         StructField("n_pages", IntegerType()), StructField("viewed_product", IntegerType()),
                         StructField("reached_cart", IntegerType()), StructField("reached_checkout", IntegerType()),
                         StructField("cart_units", LongType())])
    return spark.createDataFrame(rows, schema)


def load_all(spark, source, with_pages=False):
    """returns users, transactions, order_lines, sessions"""
    if source == "files":
        return _files(spark, with_pages)
    users, tx, lines = _mongo(spark)
    sessions = _hbase(spark, with_pages)
    # in db mode only the HBase subset of sessions is loaded - restrict tx accordingly where needed
    return users, tx, lines, sessions

