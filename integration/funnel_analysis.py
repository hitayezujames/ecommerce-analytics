"""
funnel_analysis.py - Part 3 integrated query #2: conversion funnel across stores.

BUSINESS QUESTION
  "Where in the journey do we lose shoppers, does it differ by device / traffic source,
   and how much revenue does each lost step represent?"

DATA SOURCES
  HBase   user_sessions (s: metadata + pv: page views + c: cart) -> stages 1-4
  MongoDB transactions (session_id link)                         -> stage 5 (paid order)
  Spark   joins sessions to orders and aggregates by segment

STAGES
  1 session started  2 viewed a product  3 added to cart  4 reached checkout  5 paid order

The paid-order stage comes from the ORDER store, not from the session's own
"conversion_status" flag - the job also reports how often those two disagree
(a cross-system reconciliation check).

Usage:  spark-submit funnel_analysis.py --source files|db
"""
import argparse

from pyspark.sql import functions as F

from sources import get_spark, load_all
from common import save_csv

STAGES = [("1 Session", "s1"), ("2 Product view", "s2"), ("3 Add to cart", "s3"),
          ("4 Checkout", "s4"), ("5 Paid order", "s5")]


def funnel(df, by=None):
    keys = [by] if by else []
    agg = df.groupBy(*keys).agg(*[F.sum(c).alias(c) for _, c in STAGES],
                                F.round(F.sum("order_value"), 2).alias("revenue"))
    for (_, prev), (_, cur) in zip(STAGES, STAGES[1:]):
        agg = agg.withColumn(f"{cur}_step_pct", F.round(F.col(cur) / F.col(prev) * 100, 1))
    return (agg.withColumn("overall_conversion_pct", F.round(F.col("s5") / F.col("s1") * 100, 2))
               .withColumn("revenue_per_session", F.round(F.col("revenue") / F.col("s1"), 2)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["files", "db"], default="files")
    args = ap.parse_args()
    spark = get_spark("integration-funnel")
    _, tx, _, sessions = load_all(spark, args.source, with_pages=True)

    orders = tx.filter(F.col("session_id").isNotNull()).select("session_id", F.col("total").alias("order_value"))
    s = (sessions.join(orders, "session_id", "left")
         .withColumn("s1", F.lit(1))
         .withColumn("s2", F.col("viewed_product"))
         .withColumn("s3", (F.col("cart_units") > 0).cast("int"))
         .withColumn("s4", ((F.col("cart_units") > 0) & (F.col("reached_checkout") == 1)).cast("int"))
         .withColumn("s5", F.col("order_value").isNotNull().cast("int"))
         .fillna({"order_value": 0.0})
         .cache())

    overall = funnel(s)
    overall.show(truncate=False)
    stages = spark.createDataFrame(
        [(name, int(overall.first()[c])) for name, c in STAGES], "stage string, sessions long")
    save_csv(stages, "integration_funnel_overall")
    save_csv(overall, "integration_funnel_summary")

    for dim in ("device_type", "referrer"):
        by = funnel(s, dim).orderBy(F.desc("overall_conversion_pct"))
        by.show(truncate=False)
        save_csv(by, f"integration_funnel_by_{dim}")

    # reconciliation: session says "converted" but the order store has no order
    recon = (s.groupBy("conversion_status")
             .agg(F.count("*").alias("sessions"), F.sum("s5").alias("with_paid_order"))
             .withColumn("without_order", F.col("sessions") - F.col("with_paid_order")))
    recon.show()
    save_csv(recon, "integration_funnel_reconciliation")
    spark.stop()


if __name__ == "__main__":
    main()
