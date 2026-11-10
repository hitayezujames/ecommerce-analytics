"""
01_clean_normalize.py - Part 2 batch job #1: raw JSON -> clean, typed Parquet star schema.

Pipeline
  raw JSON (multi-line arrays)
    -> enforce schema, trim strings, normalise case of enumerations
    -> parse ISO strings into timestamps, derive date/month columns
    -> de-duplicate on business keys, drop rows without keys
    -> repair inconsistencies (durations, last_active < registration, totals)
    -> explode nested arrays into fact tables (page views, cart lines, order lines)
    -> enrich order lines with category / subcategory / margin (broadcast join)
    -> write Parquet (columnar + snappy; page views partitioned by event_date)
    -> data_quality_report.json with row counts and every issue found

Outputs (data/processed/):
  dim_users, dim_categories, dim_subcategories, dim_products, product_price_history,
  fact_sessions, fact_page_views (partitioned by event_date), fact_cart_items,
  fact_transactions, fact_transaction_items
"""
import json
import os

from pyspark.sql import functions as F
from pyspark.sql import Window

from common import (CATEGORIES_SCHEMA, PRODUCTS_SCHEMA, SESSIONS_SCHEMA, TRANSACTIONS_SCHEMA,
                    USERS_SCHEMA, config, get_spark, processed, read_raw)

dq = {}  # data-quality report collected along the way


def clean_str(col):
    """trim + empty string -> NULL"""
    return F.when(F.trim(col) == "", None).otherwise(F.trim(col))


def write(df, name, partition_by=None):
    w = df.write.mode("overwrite")
    if partition_by:
        w = w.partitionBy(partition_by)
    w.parquet(processed(name))
    n = df.sparkSession.read.parquet(processed(name)).count()
    dq.setdefault("rows_written", {})[name] = n
    print(f"  {name:<24} {n:>12,} rows")


def main():
    spark = get_spark("01-clean-normalize")

    # ------------------------------------------------------------------ users
    raw_users = read_raw(spark, "users", USERS_SCHEMA)
    users = (raw_users
             .select(clean_str(F.col("user_id")).alias("user_id"),
                     F.initcap(clean_str(F.col("geo_data.city"))).alias("city"),
                     F.upper(clean_str(F.col("geo_data.state"))).alias("state"),
                     F.upper(clean_str(F.col("geo_data.country"))).alias("country"),
                     F.to_timestamp("registration_date").alias("registration_ts"),
                     F.to_timestamp("last_active").alias("last_active_ts"))
             .dropna(subset=["user_id"])
             .dropDuplicates(["user_id"]))
    dq["users_last_active_before_registration"] = users.filter(
        F.col("last_active_ts") < F.col("registration_ts")).count()
    users = (users
             .withColumn("last_active_ts", F.greatest("last_active_ts", "registration_ts"))
             .withColumn("country", F.coalesce("country", F.lit("UNKNOWN")))
             .withColumn("registration_month", F.date_format("registration_ts", "yyyy-MM")))
    dq["users_raw"] = raw_users.count()

    # ------------------------------------------------------- categories (dims)
    cats = read_raw(spark, "categories", CATEGORIES_SCHEMA).dropDuplicates(["category_id"])
    dim_categories = cats.select("category_id", F.trim("name").alias("category_name"))
    dim_subcategories = (cats
                         .select("category_id", F.explode_outer("subcategories").alias("s"))
                         .select(F.col("s.subcategory_id").alias("subcategory_id"), "category_id",
                                 F.trim("s.name").alias("subcategory_name"),
                                 F.col("s.profit_margin").alias("profit_margin"))
                         .dropna(subset=["subcategory_id"]))
    cat_avg_margin = dim_subcategories.groupBy("category_id").agg(
        F.round(F.avg("profit_margin"), 3).alias("category_avg_margin"))

    # ------------------------------------------------------------- products
    raw_products = read_raw(spark, "products", PRODUCTS_SCHEMA)
    dq["products_missing_subcategory"] = raw_products.filter(F.col("subcategory_id").isNull()).count()
    products = (raw_products
                .dropna(subset=["product_id"]).dropDuplicates(["product_id"])
                .withColumn("product_name", F.initcap(clean_str(F.col("name"))))
                .withColumn("creation_ts", F.to_timestamp("creation_date"))
                .withColumn("n_price_points", F.size("price_history"))
                .withColumn("min_price", F.array_min(F.col("price_history.price")))
                .withColumn("max_price", F.array_max(F.col("price_history.price")))
                .withColumn("current_stock", F.greatest(F.col("current_stock"), F.lit(0)))
                .withColumn("is_active", F.coalesce("is_active", F.lit(False))))
    # margin: subcategory margin when known, otherwise the category average (original generator)
    dim_products = (products
                    .join(F.broadcast(dim_subcategories.select("subcategory_id", "profit_margin")),
                          "subcategory_id", "left")
                    .join(F.broadcast(cat_avg_margin), "category_id", "left")
                    .withColumn("profit_margin", F.coalesce("profit_margin", "category_avg_margin"))
                    .join(F.broadcast(dim_categories), "category_id", "left")
                    .select("product_id", "product_name", "category_id", "category_name", "subcategory_id",
                            "profit_margin", F.col("base_price").alias("current_price"), "min_price",
                            "max_price", "n_price_points", "current_stock", "is_active", "creation_ts"))
    price_history = (products
                     .select("product_id", F.explode("price_history").alias("h"))
                     .select("product_id", F.col("h.price").alias("price"),
                             F.to_timestamp("h.date").alias("effective_ts"))
                     .withColumn("prev_price", F.lag("price").over(
                         Window.partitionBy("product_id").orderBy("effective_ts")))
                     .withColumn("pct_change", F.round((F.col("price") / F.col("prev_price") - 1) * 100, 2)))

    # ------------------------------------------------------------- sessions
    raw_sessions = read_raw(spark, "sessions", SESSIONS_SCHEMA)
    s = (raw_sessions
         .dropna(subset=["session_id", "user_id", "start_time"])
         .dropDuplicates(["session_id"])
         .withColumn("start_ts", F.to_timestamp("start_time"))
         .withColumn("end_ts", F.to_timestamp("end_time")))
    s = s.withColumn("computed_duration", F.col("end_ts").cast("long") - F.col("start_ts").cast("long"))
    dq["sessions_duration_mismatch"] = s.filter(
        F.col("duration_seconds").isNull() | (F.col("duration_seconds") != F.col("computed_duration"))).count()
    s = s.withColumn("duration_seconds", F.coalesce(F.col("computed_duration"), F.col("duration_seconds"))).cache()

    page_views = (s.select("session_id", "user_id", F.posexplode("page_views").alias("seq", "pv"))
                  .select("session_id", "user_id", "seq",
                          F.to_timestamp("pv.timestamp").alias("event_ts"),
                          F.lower(clean_str(F.col("pv.page_type"))).alias("page_type"),
                          clean_str(F.col("pv.product_id")).alias("product_id"),
                          clean_str(F.col("pv.category_id")).alias("category_id"),
                          F.coalesce(F.col("pv.view_duration"), F.lit(0)).alias("view_duration"))
                  .withColumn("event_date", F.to_date("event_ts")))

    cart_items = (s.select("session_id", "user_id", "start_ts", F.explode("cart_contents").alias("product_id", "c"))
                  .select("session_id", "user_id", "start_ts", "product_id",
                          F.col("c.quantity").alias("quantity"), F.col("c.price").alias("price"))
                  .filter(F.col("quantity") > 0))

    # per-session funnel flags computed from the exploded page views
    flags = (page_views.groupBy("session_id").agg(
        F.count("*").alias("n_pages"),
        F.max(F.when(F.col("page_type") == "product_detail", 1).otherwise(0)).alias("viewed_product"),
        F.max(F.when(F.col("page_type") == "cart", 1).otherwise(0)).alias("reached_cart"),
        F.max(F.when(F.col("page_type") == "checkout", 1).otherwise(0)).alias("reached_checkout"),
        F.max(F.when(F.col("page_type") == "confirmation", 1).otherwise(0)).alias("reached_confirmation")))
    cart_value = cart_items.groupBy("session_id").agg(
        F.sum("quantity").alias("cart_units"), F.round(F.sum(F.col("quantity") * F.col("price")), 2).alias("cart_value"))

    fact_sessions = (s.select("session_id", "user_id", "start_ts", "end_ts", "duration_seconds",
                              F.to_date("start_ts").alias("session_date"),
                              F.upper(clean_str(F.col("geo_data.country"))).alias("country"),
                              F.lower(clean_str(F.col("device_profile.type"))).alias("device_type"),
                              F.col("device_profile.os").alias("os"),
                              F.col("device_profile.browser").alias("browser"),
                              F.lower(clean_str(F.col("referrer"))).alias("referrer"),
                              F.lower(clean_str(F.col("conversion_status"))).alias("conversion_status"),
                              F.size("viewed_products").alias("n_products_viewed"))
                     .join(flags, "session_id", "left")
                     .join(cart_value, "session_id", "left")
                     .fillna({"n_pages": 0, "viewed_product": 0, "reached_cart": 0, "reached_checkout": 0,
                              "reached_confirmation": 0, "cart_units": 0, "cart_value": 0.0}))

    # --------------------------------------------------------- transactions
    raw_tx = read_raw(spark, "transactions", TRANSACTIONS_SCHEMA)
    tx = (raw_tx.dropna(subset=["transaction_id", "user_id", "timestamp"])
          .dropDuplicates(["transaction_id"])
          .withColumn("txn_ts", F.to_timestamp("timestamp"))
          .withColumn("items", F.filter("items", lambda i: (i["quantity"] > 0) & (i["unit_price"] >= 0)))
          .withColumn("items_subtotal", F.round(F.aggregate(
              "items", F.lit(0.0), lambda acc, i: acc + i["quantity"] * i["unit_price"]), 2))
          .withColumn("discount", F.coalesce("discount", F.lit(0.0))))
    dq["transactions_subtotal_mismatch"] = tx.filter(F.abs(F.col("items_subtotal") - F.col("subtotal")) > 0.02).count()
    dq["transactions_total_mismatch"] = tx.filter(
        F.abs(F.col("total") - (F.col("subtotal") - F.col("discount"))) > 0.02).count()
    fact_tx = (tx.select("transaction_id", clean_str(F.col("session_id")).alias("session_id"), "user_id", "txn_ts",
                         F.to_date("txn_ts").alias("txn_date"),
                         F.date_format("txn_ts", "yyyy-MM").alias("txn_month"),
                         F.col("items_subtotal").alias("subtotal"), "discount",
                         F.round(F.col("items_subtotal") - F.col("discount"), 2).alias("total"),
                         F.lower(clean_str(F.col("payment_method"))).alias("payment_method"),
                         F.coalesce(F.lower(clean_str(F.col("status"))), F.lit("unknown")).alias("status"),
                         F.size("items").alias("n_items"))
               .withColumn("channel", F.when(F.col("session_id").isNotNull(), "web_session").otherwise("other"))
               .filter(F.col("n_items") > 0))

    items = (tx.select("transaction_id", "user_id", "txn_ts", "subtotal", "total", F.explode("items").alias("i"))
             .select("transaction_id", "user_id", "txn_ts", F.col("i.product_id").alias("product_id"),
                     F.col("i.quantity").alias("quantity"), F.col("i.unit_price").alias("unit_price"),
                     F.round(F.col("i.quantity") * F.col("i.unit_price"), 2).alias("line_subtotal"),
                     # order-level discount spread pro-rata over the lines
                     F.when(F.col("subtotal") > 0, F.col("total") / F.col("subtotal")).otherwise(1.0).alias("net_factor")))
    fact_items = (items.join(F.broadcast(dim_products.select("product_id", "category_id", "subcategory_id",
                                                             "profit_margin")), "product_id", "left")
                  .withColumn("net_revenue", F.round(F.col("line_subtotal") * F.col("net_factor"), 2))
                  .withColumn("gross_profit", F.round(F.col("net_revenue") * F.coalesce("profit_margin", F.lit(0.0)), 2))
                  .withColumn("txn_date", F.to_date("txn_ts"))
                  .drop("net_factor"))

    # ----------------------------------------------------- referential checks
    dq["transactions_unknown_user"] = fact_tx.join(users, "user_id", "left_anti").count()
    dq["order_lines_unknown_product"] = fact_items.join(dim_products, "product_id", "left_anti").count()
    dq["sessions_unknown_user"] = fact_sessions.join(users, "user_id", "left_anti").count()
    dq["converted_sessions_without_transaction"] = (
        fact_sessions.filter(F.col("conversion_status") == "converted")
        .join(fact_tx.select("session_id").dropna(), "session_id", "left_anti").count())
    dq["inactive_products_sold_lines"] = (fact_items.join(dim_products.filter(~F.col("is_active")), "product_id")
                                          .count())

    # ---------------------------------------------------------------- write
    print("Writing Parquet:")
    write(users, "dim_users")
    write(dim_categories, "dim_categories")
    write(dim_subcategories, "dim_subcategories")
    write(dim_products, "dim_products")
    write(price_history, "product_price_history")
    write(fact_sessions, "fact_sessions")
    write(page_views, "fact_page_views", partition_by="event_date")  # enables date pruning
    write(cart_items, "fact_cart_items")
    write(fact_tx, "fact_transactions")
    write(fact_items, "fact_transaction_items")

    with open(os.path.join(config.OUTPUT_DIR, "data_quality_report.json"), "w") as f:
        json.dump(dq, f, indent=2)
    print("Data quality report:", json.dumps(dq, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
