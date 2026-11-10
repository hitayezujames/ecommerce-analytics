"""
03_cohort_analysis.py - Part 2 batch job #3: cohort analysis of purchasing patterns.

Two complementary cohort definitions:

 1. REGISTRATION cohort (the brief's example): users grouped by registration month;
    for each cohort and each calendar month of activity we compute active buyers,
    % of the cohort buying, revenue and revenue per cohort member.
    Note: in this dataset all registrations are 90-270 days before the end date and all
    orders fall in the last 90 days, so "months since registration" starts at ~3.

 2. ACQUISITION cohort (first-purchase month): the classic retention matrix -
    of the customers whose first order was in month M, what % ordered again in
    month M+1, M+2, ...  This is the view a marketing team acts on.

Outputs: cohort_registration.csv, cohort_retention_matrix.csv, cohort_revenue_matrix.csv
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import drop_partial_trailing_month, get_spark, read_processed, save_csv


def months_between(later, earlier):
    """whole calendar months between two 'yyyy-MM' month starts"""
    return F.months_between(F.to_date(later), F.to_date(earlier)).cast("int")


def main():
    spark = get_spark("03-cohort-analysis")
    users = read_processed(spark, "dim_users")
    tx = drop_partial_trailing_month(read_processed(spark, "fact_transactions"), "txn_ts")

    tx_m = tx.withColumn("activity_month", F.trunc("txn_date", "month"))
    users_m = users.withColumn("cohort_month", F.trunc(F.to_date("registration_ts"), "month"))

    # ---------------------------------------------------- 1. registration cohorts
    cohort_size = users_m.groupBy("cohort_month").agg(F.count("*").alias("cohort_size"))
    reg = (tx_m.join(users_m.select("user_id", "cohort_month"), "user_id")
           .groupBy("cohort_month", "activity_month")
           .agg(F.countDistinct("user_id").alias("active_buyers"),
                F.count("*").alias("orders"),
                F.round(F.sum("total"), 2).alias("revenue"))
           .join(cohort_size, "cohort_month")
           .withColumn("months_since_registration", months_between("activity_month", "cohort_month"))
           .withColumn("pct_cohort_active", F.round(F.col("active_buyers") / F.col("cohort_size") * 100, 1))
           .withColumn("revenue_per_member", F.round(F.col("revenue") / F.col("cohort_size"), 2))
           .withColumn("cum_revenue_per_member", F.round(F.sum("revenue_per_member").over(
               Window.partitionBy("cohort_month").orderBy("activity_month")
               .rowsBetween(Window.unboundedPreceding, Window.currentRow)), 2))
           .orderBy("cohort_month", "activity_month"))
    reg.show(40, truncate=False)
    save_csv(reg.withColumn("cohort_month", F.date_format("cohort_month", "yyyy-MM"))
                .withColumn("activity_month", F.date_format("activity_month", "yyyy-MM")), "cohort_registration")

    # ---------------------------------------------------- 2. acquisition cohorts
    first = tx_m.groupBy("user_id").agg(F.min("activity_month").alias("first_month"))
    activity = (tx_m.join(first, "user_id")
                .withColumn("period", months_between("activity_month", "first_month")))
    sizes = first.groupBy("first_month").agg(F.count("*").alias("new_customers"))
    matrix = (activity.groupBy("first_month", "period")
              .agg(F.countDistinct("user_id").alias("customers"), F.round(F.sum("total"), 2).alias("revenue"))
              .join(sizes, "first_month")
              .withColumn("retention_pct", F.round(F.col("customers") / F.col("new_customers") * 100, 1))
              .withColumn("first_month", F.date_format("first_month", "yyyy-MM")))

    retention = (matrix.groupBy("first_month", "new_customers").pivot("period").agg(F.first("retention_pct"))
                 .orderBy("first_month"))
    retention.show(truncate=False)
    save_csv(retention, "cohort_retention_matrix")

    revenue = (matrix.withColumn("rev_per_customer", F.round(F.col("revenue") / F.col("new_customers"), 2))
               .groupBy("first_month", "new_customers").pivot("period").agg(F.first("rev_per_customer"))
               .orderBy("first_month"))
    revenue.show(truncate=False)
    save_csv(revenue, "cohort_revenue_matrix")

    # days to second purchase - a useful KPI for reactivation campaigns
    w = Window.partitionBy("user_id").orderBy("txn_ts")
    gaps = (tx.withColumn("prev_ts", F.lag("txn_ts").over(w))
            .withColumn("order_no", F.row_number().over(w))
            .filter(F.col("order_no") == 2)
            .withColumn("days_to_2nd", (F.col("txn_ts").cast("long") - F.col("prev_ts").cast("long")) / 86400))
    gaps.select(F.round(F.avg("days_to_2nd"), 1).alias("avg_days_to_2nd_order"),
                F.percentile_approx("days_to_2nd", 0.5).alias("median_days_to_2nd_order"),
                F.count("*").alias("repeat_customers")).show()
    spark.stop()


if __name__ == "__main__":
    main()
