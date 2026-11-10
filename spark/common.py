"""
common.py - SparkSession factory, explicit schemas for the raw JSON, and readers.

Why explicit schemas?  Schema inference on multi-line JSON makes Spark read every
file twice (once to infer, once to load) - on ~4 GB of sessions that doubles I/O.
Explicit schemas also pin types (e.g. discount is sometimes the integer 0 in the raw
JSON) and make missing fields (subcategory_id in the original generator) come back
as NULL instead of breaking the job.
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, LongType, MapType,
                               StringType, StructField, StructType)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config  # noqa: E402,F401  (re-exported for the jobs)


def _total_ram_gb():
    try:
        with open("/proc/meminfo") as f:
            return int(f.readline().split()[1]) / 1048576
    except OSError:
        return 8.0


LOW_MEMORY = _total_ram_gb() < 6     # e.g. a 4 GB laptop running WSL


def get_spark(app_name):
    if LOW_MEMORY:
        print(f"[low-memory mode] {_total_ram_gb():.1f} GB RAM detected: driver 1g, 8 shuffle partitions")
    spark = (SparkSession.builder
             .appName(app_name)
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g" if LOW_MEMORY else "4g"))
             .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "8" if LOW_MEMORY else "64"))
             .config("spark.sql.adaptive.enabled", "true")                     # AQE: coalesce partitions, fix skew
             .config("spark.sql.adaptive.skewJoin.enabled", "true")
             .config("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))  # dims are tiny
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false" if LOW_MEMORY else "true")
             .config("spark.sql.parquet.compression.codec", "snappy")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


S = lambda *f: StructType(list(f))  # noqa: E731
F = lambda n, t, nullable=True: StructField(n, t, nullable)  # noqa: E731

USERS_SCHEMA = S(
    F("user_id", StringType()),
    F("geo_data", S(F("city", StringType()), F("state", StringType()), F("country", StringType()))),
    F("registration_date", StringType()),
    F("last_active", StringType()))

CATEGORIES_SCHEMA = S(
    F("category_id", StringType()),
    F("name", StringType()),
    F("subcategories", ArrayType(S(F("subcategory_id", StringType()), F("name", StringType()),
                                   F("profit_margin", DoubleType())))))

PRODUCTS_SCHEMA = S(
    F("product_id", StringType()),
    F("name", StringType()),
    F("category_id", StringType()),
    F("subcategory_id", StringType()),
    F("base_price", DoubleType()),
    F("current_stock", LongType()),
    F("is_active", BooleanType()),
    F("price_history", ArrayType(S(F("price", DoubleType()), F("date", StringType())))),
    F("creation_date", StringType()))

SESSIONS_SCHEMA = S(
    F("session_id", StringType()),
    F("user_id", StringType()),
    F("start_time", StringType()),
    F("end_time", StringType()),
    F("duration_seconds", LongType()),
    F("geo_data", S(F("city", StringType()), F("state", StringType()),
                    F("country", StringType()), F("ip_address", StringType()))),
    F("device_profile", S(F("type", StringType()), F("os", StringType()), F("browser", StringType()))),
    F("viewed_products", ArrayType(StringType())),
    F("page_views", ArrayType(S(F("timestamp", StringType()), F("page_type", StringType()),
                                F("product_id", StringType()), F("category_id", StringType()),
                                F("view_duration", LongType())))),
    F("cart_contents", MapType(StringType(), S(F("quantity", LongType()), F("price", DoubleType())))),
    F("conversion_status", StringType()),
    F("referrer", StringType()))

TRANSACTIONS_SCHEMA = S(
    F("transaction_id", StringType()),
    F("session_id", StringType()),
    F("user_id", StringType()),
    F("timestamp", StringType()),
    F("items", ArrayType(S(F("product_id", StringType()), F("quantity", LongType()),
                           F("unit_price", DoubleType()), F("subtotal", DoubleType())))),
    F("subtotal", DoubleType()),
    F("discount", DoubleType()),
    F("total", DoubleType()),
    F("payment_method", StringType()),
    F("status", StringType()))


def read_raw(spark, name, schema):
    """Raw files are JSON *arrays* (not JSON-lines) -> multiLine=True.
    PERMISSIVE mode keeps malformed records as NULL rows that the cleaning step removes."""
    path = os.path.join(config.RAW_DIR, f"{name}.json" if name != "sessions" else "sessions_*.json")
    return (spark.read.schema(schema)
            .option("multiLine", True)
            .option("mode", "PERMISSIVE")
            .json(path))


def processed(name):
    return os.path.join(config.PROCESSED_DIR, name)


def read_processed(spark, name):
    return spark.read.parquet(processed(name))


def drop_partial_trailing_month(df, ts_col, min_days=28):
    """Monthly metrics on a month with only a few hours of data look like a collapse in
    retention. If the last month is observed for fewer than `min_days`, exclude it."""
    from pyspark.sql import functions as F  # local import keeps module import light
    max_ts = df.agg(F.max(ts_col)).first()[0]
    if max_ts is not None and max_ts.day < min_days:
        cutoff = max_ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        print(f"  (excluding partial month starting {cutoff.date()} - only {max_ts.day} day(s) observed)")
        return df.filter(F.col(ts_col) < F.lit(cutoff))
    return df


def save_csv(df, name):
    """Small result sets -> single CSV via pandas (avoids Spark's part-files folder)."""
    out = os.path.join(config.OUTPUT_DIR, f"{name}.csv")
    df.toPandas().to_csv(out, index=False)
    print(f"  -> {out}")
    return out
