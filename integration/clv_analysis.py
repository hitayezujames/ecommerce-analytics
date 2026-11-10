"""
clv_analysis.py - Part 3 integrated query: Customer Lifetime Value + engagement segmentation.

BUSINESS QUESTION
  "Which customers will be worth the most over the next 12 months, how concentrated is
   that value, and which engaged users are under-monetised (browse a lot, buy little)?"

DATA SOURCES (why each store)
  MongoDB  users         -> profile (country, registration)          [document lookups by user]
  MongoDB  transactions  -> orders + embedded lines with margin       [rich nested order docs]
  HBase    user_sessions -> engagement: #sessions, duration, pages    [time-series scan per user]
  Spark                   -> joins the three, estimates retention, scores CLV, segments

METHOD
  1. per user value  : orders, revenue, gross profit (order lines x subcategory margin)
  2. per user engagement : sessions, avg minutes, pages, sessions that built a cart
  3. monthly retention r : P(customer buys in month m+1 | bought in month m), from the data
  4. CLV_12m = monthly_gross_profit x sum_{t=0..11} (r / (1+d))^t     (d = 1% monthly discount)
     - a standard retention-based CLV (Gupta & Lehmann, 2003), using margin not revenue.
  5. value tiers (CLV quintiles), Pareto share, and an engagement x value quadrant map.

Usage:  spark-submit clv_analysis.py --source files|db
"""
import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from sources import get_spark, load_all
from common import drop_partial_trailing_month, save_csv

MONTHLY_DISCOUNT = 0.01
HORIZON_MONTHS = 12


def monthly_retention(tx):
    """average share of month-m buyers who buy again in month m+1"""
    tx = drop_partial_trailing_month(tx, "txn_ts")  # a few hours of a new month would fake churn
    active = tx.select("user_id", F.trunc(F.to_date("txn_ts"), "month").alias("m")).distinct()
    nxt = active.select("user_id", F.add_months("m", -1).alias("m"))  # shift m+1 back onto m
    per_month = (active.join(nxt.withColumn("again", F.lit(1)), ["user_id", "m"], "left")
                 .groupBy("m").agg(F.count("*").alias("buyers"), F.sum(F.coalesce("again", F.lit(0))).alias("again")))
    last_m = active.agg(F.max("m")).first()[0]
    per_month = per_month.filter(F.col("m") < last_m)  # last month has no "next month" yet
    per_month.orderBy("m").show()
    r = per_month.agg((F.sum("again") / F.sum("buyers")).alias("r")).first()["r"]
    return float(r)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["files", "db"], default="files")
    args = ap.parse_args()
    spark = get_spark("integration-clv")
    users, tx, lines, sessions = load_all(spark, args.source)

    # ---------- 1. value per user (MongoDB data) ----------
    profit = lines.groupBy("user_id").agg(F.round(F.sum("gross_profit"), 2).alias("gross_profit"))
    value = (tx.groupBy("user_id").agg(F.count("*").alias("orders"),
                                       F.round(F.sum("total"), 2).alias("revenue"),
                                       F.min("txn_ts").alias("first_order"), F.max("txn_ts").alias("last_order"))
             .join(profit, "user_id", "left"))

    # ---------- 2. engagement per user (HBase data) ----------
    engagement = sessions.groupBy("user_id").agg(
        F.count("*").alias("sessions"),
        F.round(F.avg("duration_seconds") / 60, 1).alias("avg_session_min"),
        F.sum("n_pages").alias("pages_viewed"),
        F.sum(F.when(F.col("cart_units") > 0, 1).otherwise(0)).alias("cart_sessions"),
        F.max("start_ts").alias("last_seen"))

    # ---------- 3. retention + observation window ----------
    r = monthly_retention(tx)
    bounds = tx.agg(F.min("txn_ts").alias("a"), F.max("txn_ts").alias("b")).first()
    obs_months = max((bounds["b"] - bounds["a"]).days / 30.44, 1.0)
    factor = sum((r / (1 + MONTHLY_DISCOUNT)) ** t for t in range(HORIZON_MONTHS))
    print(f"monthly retention r={r:.3f}, observation window={obs_months:.2f} months, CLV multiplier={factor:.2f}")

    # ---------- 4. join everything & score ----------
    end_ts = bounds["b"]
    clv = (users.join(value, "user_id", "left").join(engagement, "user_id", "left")
           .fillna({"orders": 0, "revenue": 0.0, "gross_profit": 0.0, "sessions": 0, "pages_viewed": 0,
                    "cart_sessions": 0, "avg_session_min": 0.0})
           .withColumn("aov", F.round(F.when(F.col("orders") > 0, F.col("revenue") / F.col("orders")), 2))
           .withColumn("monthly_gross_profit", F.round(F.col("gross_profit") / obs_months, 2))
           .withColumn("clv_12m", F.round(F.col("monthly_gross_profit") * F.lit(factor), 2))
           .withColumn("recency_days", F.datediff(F.lit(end_ts), F.col("last_order")))
           .withColumn("revenue_per_session", F.round(F.when(F.col("sessions") > 0,
                                                              F.col("revenue") / F.col("sessions")), 2)))

    # CLV quintiles (10k users -> a global window is fine; at 10M users use percent_rank on a sample)
    clv = clv.withColumn("value_tier", F.concat(F.lit("Q"), (6 - F.ntile(5).over(Window.orderBy("clv_12m"))).cast("string")))

    # engagement x value quadrants around the medians
    med_s, med_v = clv.approxQuantile(["sessions", "clv_12m"], [0.5], 0.001)
    clv = clv.withColumn("quadrant", F.when((F.col("sessions") >= med_s[0]) & (F.col("clv_12m") >= med_v[0]), "Engaged high-value")
                         .when(F.col("clv_12m") >= med_v[0], "Efficient buyers (low engagement)")
                         .when(F.col("sessions") >= med_s[0], "Window shoppers (high engagement, low value)")
                         .otherwise("Dormant / low value")).cache()

    total_clv = clv.agg(F.sum("clv_12m")).first()[0]
    tiers = (clv.groupBy("value_tier").agg(
        F.count("*").alias("users"), F.round(F.avg("clv_12m"), 2).alias("avg_clv_12m"),
        F.round(F.sum("clv_12m") / total_clv * 100, 1).alias("share_of_clv_pct"),
        F.round(F.avg("orders"), 2).alias("avg_orders"), F.round(F.avg("aov"), 2).alias("avg_aov"),
        F.round(F.avg("sessions"), 1).alias("avg_sessions"),
        F.round(F.avg("revenue_per_session"), 2).alias("avg_revenue_per_session"))
        .orderBy("value_tier"))
    tiers.show(truncate=False)
    quadrants = (clv.groupBy("quadrant").agg(
        F.count("*").alias("users"), F.round(F.avg("clv_12m"), 2).alias("avg_clv_12m"),
        F.round(F.avg("sessions"), 1).alias("avg_sessions"), F.round(F.avg("cart_sessions"), 1).alias("avg_cart_sessions"),
        F.round(F.avg("orders"), 2).alias("avg_orders")).orderBy(F.desc("avg_clv_12m")))
    quadrants.show(truncate=False)

    top20 = clv.withColumn("pr", F.percent_rank().over(Window.orderBy(F.desc("clv_12m"))))
    top20_share = top20.filter(F.col("pr") < 0.2).agg(F.sum("clv_12m")).first()[0] / total_clv
    corr = clv.stat.corr("sessions", "revenue")
    summary = spark.createDataFrame([(args.source, round(r, 4), round(obs_months, 2), round(factor, 3),
                                      round(total_clv, 2), round(top20_share * 100, 1), round(corr, 4),
                                      clv.filter(F.col("orders") == 0).count())],
                                    "source string, monthly_retention double, observation_months double, "
                                    "clv_multiplier double, total_clv_12m double, top20pct_share_of_clv double, "
                                    "corr_sessions_revenue double, users_without_orders long")
    summary.show(truncate=False)

    save_csv(clv.select("user_id", "country", "orders", "revenue", "gross_profit", "aov", "sessions",
                        "avg_session_min", "cart_sessions", "recency_days", "monthly_gross_profit", "clv_12m",
                        "value_tier", "quadrant"), "integration_clv_users")
    save_csv(tiers, "integration_clv_tiers")
    save_csv(quadrants, "integration_clv_quadrants")
    save_csv(summary, "integration_clv_summary")
    spark.stop()


if __name__ == "__main__":
    main()
