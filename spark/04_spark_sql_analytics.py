"""
04_spark_sql_analytics.py - Part 2: Spark SQL analytics.

Registers the cleaned Parquet tables as SQL views and runs complex queries
(CTEs, window functions, conditional aggregation, multi-way joins).

The last two queries show how Spark SQL queries data that NOTIONALLY lives in the
operational stores: `mongo_users` / `mongo_transactions` stand for the MongoDB
collections and `hbase_sessions` for the HBase clickstream table. Here they are
loaded from Parquet; in integration/clv_analysis.py --source db they are loaded
from the real databases and the same SQL runs unchanged.

Outputs: sql_<name>.csv in data/output/
"""
from pyspark.sql import functions as F

from common import get_spark, read_processed, save_csv

QUERIES = {
    # 1. Category revenue, profit and rank, with each category's share of the total
    "category_performance": """
        WITH cat AS (
            SELECT p.category_name,
                   COUNT(DISTINCT i.transaction_id)  AS orders,
                   SUM(i.quantity)                   AS units,
                   ROUND(SUM(i.net_revenue), 2)      AS net_revenue,
                   ROUND(SUM(i.gross_profit), 2)     AS gross_profit
            FROM fact_transaction_items i
            JOIN dim_products p ON i.product_id = p.product_id
            GROUP BY p.category_name)
        SELECT *,
               ROUND(gross_profit / net_revenue * 100, 1)                     AS margin_pct,
               ROUND(net_revenue / SUM(net_revenue) OVER () * 100, 2)         AS revenue_share_pct,
               RANK() OVER (ORDER BY net_revenue DESC)                        AS revenue_rank,
               RANK() OVER (ORDER BY gross_profit DESC)                       AS profit_rank
        FROM cat
        ORDER BY net_revenue DESC""",

    # 2. Daily revenue with 7-day moving average and week-over-week change
    "daily_revenue_trend": """
        WITH d AS (
            SELECT txn_date, COUNT(*) AS orders, ROUND(SUM(total), 2) AS revenue,
                   ROUND(SUM(CASE WHEN channel = 'web_session' THEN total ELSE 0 END), 2) AS web_revenue
            FROM fact_transactions
            WHERE status <> 'unknown'
            GROUP BY txn_date)
        SELECT txn_date, orders, revenue, web_revenue,
               ROUND(AVG(revenue) OVER (ORDER BY txn_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS revenue_7d_ma,
               ROUND((revenue / LAG(revenue, 7) OVER (ORDER BY txn_date) - 1) * 100, 1)                 AS wow_change_pct,
               date_format(txn_date, 'E') AS weekday
        FROM d ORDER BY txn_date""",

    # 3. Conversion by device and referrer (sessions with a real linked order)
    "conversion_by_device_referrer": """
        SELECT s.device_type, s.referrer,
               COUNT(*)                                             AS sessions,
               SUM(s.reached_cart)                                  AS reached_cart,
               SUM(s.reached_checkout)                              AS reached_checkout,
               COUNT(t.transaction_id)                              AS orders,
               ROUND(COUNT(t.transaction_id) / COUNT(*) * 100, 2)   AS conversion_pct,
               ROUND(AVG(t.total), 2)                               AS avg_order_value,
               ROUND(SUM(COALESCE(t.total, 0)) / COUNT(*), 2)       AS revenue_per_session
        FROM fact_sessions s
        LEFT JOIN fact_transactions t ON s.session_id = t.session_id
        GROUP BY s.device_type, s.referrer
        ORDER BY conversion_pct DESC""",

    # 4. Most viewed vs best selling products: view-to-purchase ratio
    "product_view_vs_sales": """
        WITH views AS (
            SELECT product_id, COUNT(*) AS views, COUNT(DISTINCT session_id) AS viewing_sessions
            FROM fact_page_views WHERE page_type = 'product_detail' AND product_id IS NOT NULL
            GROUP BY product_id),
        carts AS (SELECT product_id, COUNT(*) AS cart_adds FROM fact_cart_items GROUP BY product_id),
        sales AS (
            SELECT product_id, SUM(quantity) AS units_sold, ROUND(SUM(net_revenue), 2) AS revenue
            FROM fact_transaction_items GROUP BY product_id)
        SELECT p.product_id, p.product_name, p.category_name, p.current_price, p.current_stock, p.is_active,
               COALESCE(v.views, 0) AS views, COALESCE(c.cart_adds, 0) AS cart_adds,
               COALESCE(s.units_sold, 0) AS units_sold, COALESCE(s.revenue, 0) AS revenue,
               ROUND(COALESCE(c.cart_adds, 0) / NULLIF(v.views, 0) * 100, 1) AS view_to_cart_pct,
               DENSE_RANK() OVER (ORDER BY COALESCE(v.views, 0) DESC)      AS views_rank,
               DENSE_RANK() OVER (ORDER BY COALESCE(s.units_sold, 0) DESC) AS sales_rank
        FROM dim_products p
        LEFT JOIN views v ON p.product_id = v.product_id
        LEFT JOIN carts c ON p.product_id = c.product_id
        LEFT JOIN sales s ON p.product_id = s.product_id""",

    # 5. Abandoned-cart value by category: what is left on the table?
    "abandoned_cart_value": """
        SELECT p.category_name,
               COUNT(DISTINCT ci.session_id)              AS abandoned_sessions,
               SUM(ci.quantity)                           AS units,
               ROUND(SUM(ci.quantity * ci.price), 2)      AS abandoned_value
        FROM fact_cart_items ci
        JOIN fact_sessions s ON ci.session_id = s.session_id AND s.conversion_status = 'abandoned'
        JOIN dim_products p  ON ci.product_id = p.product_id
        GROUP BY p.category_name
        ORDER BY abandoned_value DESC""",

    # 6. Price elasticity hint: did products that got cheaper sell more?
    "price_change_effect": """
        WITH last_change AS (
            SELECT product_id, pct_change,
                   ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY effective_ts DESC) AS rn
            FROM product_price_history WHERE pct_change IS NOT NULL)
        SELECT CASE WHEN lc.pct_change IS NULL THEN 'no change'
                    WHEN lc.pct_change < -5  THEN 'price cut >5%'
                    WHEN lc.pct_change >  5  THEN 'price rise >5%'
                    ELSE 'small change (+/-5%)' END        AS last_price_move,
               COUNT(*)                                     AS products,
               ROUND(AVG(COALESCE(s.units, 0)), 2)          AS avg_units_sold
        FROM dim_products p
        LEFT JOIN (SELECT * FROM last_change WHERE rn = 1) lc ON p.product_id = lc.product_id
        LEFT JOIN (SELECT product_id, SUM(quantity) AS units FROM fact_transaction_items GROUP BY product_id) s
               ON p.product_id = s.product_id
        GROUP BY 1 ORDER BY 1""",

    # 7. "Cross-store" query: MongoDB users + MongoDB transactions + HBase sessions
    "cross_store_user_value_engagement": """
        WITH engagement AS (                       -- from HBase user_sessions (s: family)
            SELECT user_id, COUNT(*) AS sessions, ROUND(AVG(duration_seconds) / 60, 1) AS avg_session_min,
                   SUM(n_pages) AS pages_viewed, MAX(start_ts) AS last_seen
            FROM hbase_sessions GROUP BY user_id),
        value AS (                                 -- from MongoDB transactions
            SELECT user_id, COUNT(*) AS orders, ROUND(SUM(total), 2) AS spent
            FROM mongo_transactions GROUP BY user_id)
        SELECT u.country,
               COUNT(*)                                        AS users,
               ROUND(AVG(e.sessions), 1)                       AS avg_sessions,
               ROUND(AVG(e.avg_session_min), 1)                AS avg_session_min,
               ROUND(AVG(COALESCE(v.orders, 0)), 2)            AS avg_orders,
               ROUND(AVG(COALESCE(v.spent, 0)), 2)             AS avg_spent,
               ROUND(SUM(COALESCE(v.spent, 0)) / SUM(e.sessions), 2) AS revenue_per_session
        FROM mongo_users u                          -- from MongoDB users
        LEFT JOIN engagement e ON u.user_id = e.user_id
        LEFT JOIN value v      ON u.user_id = v.user_id
        GROUP BY u.country
        HAVING COUNT(*) >= 25
        ORDER BY avg_spent DESC""",

    # 8. Reconciliation across stores: sessions flagged converted with no order in MongoDB
    "converted_without_order": """
        WITH mx AS (SELECT MAX(session_date) AS max_day FROM hbase_sessions)
        SELECT CASE WHEN s.session_date >= date_sub(mx.max_day, 30) THEN 'last 30 days'
                    ELSE 'earlier' END                                    AS period,
               COUNT(*)                                                   AS converted_sessions,
               SUM(CASE WHEN t.transaction_id IS NULL THEN 1 ELSE 0 END)  AS without_order,
               ROUND(SUM(CASE WHEN t.transaction_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 1) AS pct_missing
        FROM hbase_sessions s
        CROSS JOIN mx
        LEFT JOIN mongo_transactions t ON s.session_id = t.session_id
        WHERE s.conversion_status = 'converted'
        GROUP BY CASE WHEN s.session_date >= date_sub(mx.max_day, 30) THEN 'last 30 days' ELSE 'earlier' END""",
}


def register_views(spark):
    for name in ("dim_users", "dim_products", "dim_categories", "fact_sessions", "fact_page_views",
                 "fact_cart_items", "fact_transactions", "fact_transaction_items", "product_price_history"):
        read_processed(spark, name).createOrReplaceTempView(name)
    # notional operational-store views (same columns the DB loaders produce)
    spark.table("dim_users").createOrReplaceTempView("mongo_users")
    spark.table("fact_transactions").createOrReplaceTempView("mongo_transactions")
    spark.table("fact_sessions").createOrReplaceTempView("hbase_sessions")


def main():
    spark = get_spark("04-spark-sql")
    register_views(spark)
    for name, sql in QUERIES.items():
        print(f"\n===== {name} =====")
        df = spark.sql(sql)
        if name == "category_performance":
            df.explain(mode="formatted")  # shows broadcast join + AQE plan for the report
        df.show(12, truncate=28)
        save_csv(df, f"sql_{name}")

    # Do the most-viewed products also sell the most?
    pv = spark.sql(QUERIES["product_view_vs_sales"])
    print("correlation(views, units_sold) =", round(pv.stat.corr("views", "units_sold"), 4))
    spark.stop()


if __name__ == "__main__":
    main()
