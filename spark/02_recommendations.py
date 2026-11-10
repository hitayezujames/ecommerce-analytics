"""
02_recommendations.py - Part 2 batch job #2: product recommendation indicators.

A) "Customers who bought X also bought Y"  (market-basket on order lines)
     support(X,Y)     = baskets containing X and Y / all baskets
     confidence(X->Y) = baskets(X,Y) / baskets(X)
     lift(X->Y)       = confidence(X->Y) / P(Y)      (>1 = bought together more than chance)
   Baskets are built at USER level (all of a user's purchases), because the generator
   puts only 1-3 random items in an order; at order level almost no pair repeats.

B) "Products frequently viewed together" (co-views within the same session)

Implementation notes / optimisation
  * pairs are generated with a self-join on basket id with a.product < b.product,
    which halves the pair space and removes self-pairs;
  * baskets with more than MAX_BASKET products are capped (the pair count grows
    quadratically and huge baskets carry little signal);
  * min-support filter is applied BEFORE computing lift so the expensive part runs
    on few rows; item frequencies are a small table and are broadcast;
  * an FP-Growth (Spark MLlib) run on order baskets is included as a cross-check.
Outputs: rec_bought_together.csv, rec_viewed_together.csv, rec_top5_per_product.csv
"""
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import get_spark, read_processed, save_csv

MIN_PAIR_COUNT_BOUGHT = 2   # raise to 3-5 on real data / the full-size dataset
MIN_PAIR_COUNT_VIEWED = 2
MAX_BASKET = 50


def pair_stats(baskets, basket_col, min_count):
    """baskets: (basket_id, product_id) distinct rows -> pair table with support/confidence/lift."""
    baskets = baskets.select(basket_col, "product_id").distinct()

    # cap very large baskets (keeps the self-join linear-ish)
    sizes = baskets.groupBy(basket_col).count().filter(F.col("count") <= MAX_BASKET).select(basket_col)
    baskets = baskets.join(sizes, basket_col).cache()

    n_baskets = baskets.select(basket_col).distinct().count()
    item_freq = baskets.groupBy("product_id").agg(F.count("*").alias("item_count"))

    a, b = baskets.alias("a"), baskets.alias("b")
    pairs = (a.join(b, (F.col(f"a.{basket_col}") == F.col(f"b.{basket_col}")) &
                    (F.col("a.product_id") < F.col("b.product_id")))
             .groupBy(F.col("a.product_id").alias("product_a"), F.col("b.product_id").alias("product_b"))
             .agg(F.count("*").alias("pair_count"))
             .filter(F.col("pair_count") >= min_count))

    fa = F.broadcast(item_freq.withColumnRenamed("product_id", "product_a").withColumnRenamed("item_count", "count_a"))
    fb = F.broadcast(item_freq.withColumnRenamed("product_id", "product_b").withColumnRenamed("item_count", "count_b"))
    return (pairs.join(fa, "product_a").join(fb, "product_b")
            .withColumn("support", F.round(F.col("pair_count") / n_baskets, 6))
            .withColumn("confidence_a_to_b", F.round(F.col("pair_count") / F.col("count_a"), 4))
            .withColumn("confidence_b_to_a", F.round(F.col("pair_count") / F.col("count_b"), 4))
            .withColumn("lift", F.round(F.col("pair_count") * n_baskets / (F.col("count_a") * F.col("count_b")), 3))
            .orderBy(F.desc("pair_count"), F.desc("lift"))), n_baskets


def main():
    spark = get_spark("02-recommendations")
    items = read_processed(spark, "fact_transaction_items")
    page_views = read_processed(spark, "fact_page_views")
    products = read_processed(spark, "dim_products").select("product_id", "product_name", "category_name")

    # ---------------- A) bought together (user-level baskets) ----------------
    bought, n_users = pair_stats(items.select(F.col("user_id").alias("basket"), "product_id"), "basket",
                                 MIN_PAIR_COUNT_BOUGHT)
    bought = bought.cache()
    print(f"Bought-together: {n_users:,} user baskets, {bought.count():,} pairs with >= {MIN_PAIR_COUNT_BOUGHT} co-buyers")
    named = (bought.join(F.broadcast(products.withColumnRenamed("product_id", "product_a")
                                     .withColumnRenamed("product_name", "name_a")
                                     .withColumnRenamed("category_name", "category_a")), "product_a")
             .join(F.broadcast(products.withColumnRenamed("product_id", "product_b")
                               .withColumnRenamed("product_name", "name_b")
                               .withColumnRenamed("category_name", "category_b")), "product_b")
             .withColumn("same_category", F.col("category_a") == F.col("category_b"))
             .orderBy(F.desc("pair_count"), F.desc("lift")))
    named.show(10, truncate=30)
    save_csv(named.limit(200), "rec_bought_together")

    # top-5 recommendations per product (both directions of each pair)
    directed = (bought.select(F.col("product_a").alias("product_id"), F.col("product_b").alias("recommended"),
                              "pair_count", F.col("confidence_a_to_b").alias("confidence"), "lift")
                .unionByName(bought.select(F.col("product_b").alias("product_id"),
                                           F.col("product_a").alias("recommended"),
                                           "pair_count", F.col("confidence_b_to_a").alias("confidence"), "lift")))
    w = Window.partitionBy("product_id").orderBy(F.desc("confidence"), F.desc("lift"), F.desc("pair_count"))
    top5 = directed.withColumn("rank", F.row_number().over(w)).filter(F.col("rank") <= 5)
    save_csv(top5.orderBy("product_id", "rank"), "rec_top5_per_product")

    # ---------------- B) viewed together (session-level) ----------------------
    views = (page_views.filter((F.col("page_type") == "product_detail") & F.col("product_id").isNotNull())
             .select("session_id", "product_id"))
    viewed, n_sessions = pair_stats(views, "session_id", MIN_PAIR_COUNT_VIEWED)
    print(f"Viewed-together: {n_sessions:,} sessions with product views")
    viewed.show(10)
    save_csv(viewed.limit(200), "rec_viewed_together")

    # ---------------- C) category affinity (dense enough to be significant) ---
    # 5,000 products give ~12.5M possible pairs, so product pairs are sparse;
    # rolling up to 25 categories gives every pair enough observations.
    cat_baskets = items.select(F.col("user_id").alias("basket"), F.col("category_id").alias("product_id")).dropna()
    cat_aff, _ = pair_stats(cat_baskets, "basket", 1)
    cats = read_processed(spark, "dim_categories")
    cat_aff = (cat_aff.join(F.broadcast(cats.toDF("product_a", "name_a")), "product_a")
               .join(F.broadcast(cats.toDF("product_b", "name_b")), "product_b")
               .orderBy(F.desc("lift")))
    cat_aff.show(10, truncate=25)
    save_csv(cat_aff, "rec_category_affinity")

    # ---------------- cross-check with MLlib FP-Growth on order baskets -------
    baskets = items.groupBy("transaction_id").agg(F.collect_set("product_id").alias("items"))
    fp = FPGrowth(itemsCol="items", minSupport=0.0001, minConfidence=0.0).fit(baskets)
    multi = fp.freqItemsets.filter(F.size("items") > 1)
    print(f"FP-Growth (order level, minSupport=0.0001): {multi.count()} frequent multi-item sets")
    multi.orderBy(F.desc("freq")).show(5, truncate=False)

    # summary numbers for the report
    summary = spark.createDataFrame([(
        n_users, bought.count(), float(bought.agg(F.avg("lift")).first()[0] or 0),
        n_sessions, viewed.count(), float(viewed.agg(F.avg("lift")).first()[0] or 0),
        multi.count())],
        "user_baskets long, bought_pairs long, bought_avg_lift double, view_sessions long, "
        "viewed_pairs long, viewed_avg_lift double, fpgrowth_multi_itemsets long")
    save_csv(summary, "rec_summary")
    spark.stop()


if __name__ == "__main__":
    main()
