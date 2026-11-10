# Distributed Multi-Model Analytics for E-commerce Data

AUCA Big Data Analytics – Final Project. MongoDB (documents) + HBase (wide-column) + Apache Spark
(distributed processing) applied to a synthetic 90-day e-commerce dataset.

```
data_generation/   dataset_generator.py (improved, reproducible) + the original for reference
mongodb/           load_mongodb.py (schema, validators, indexes, $merge summaries) · aggregations.py
hbase/             create_schema.hbase · create_schema.py · load_hbase.py · query_hbase.py · sample_queries.hbase · hbase_utils.py
spark/             common.py · 01_clean_normalize · 02_recommendations · 03_cohort_analysis · 04_spark_sql_analytics
integration/       sources.py (files|db back-ends) · clv_analysis.py · funnel_analysis.py
visualization/     make_charts.py -> report/figures/*.png
report/            Technical_Report.pdf + figures + build_report.py
setup/             setup_ubuntu.sh · start_services.sh · stop_services.sh
run_all.py         runs the whole pipeline (Windows/WSL, macOS, Linux)
data/              raw/ (generator output) · processed/ (Parquet) · output/ (result CSVs)
```

## 1. Installation
**See [SETUP_GUIDE.md](SETUP_GUIDE.md)** – installs Java 17, Python/PySpark, MongoDB and HBase directly on your
computer (Windows via WSL Ubuntu, macOS via Homebrew, or Linux). On Ubuntu/WSL it is one command:
```bash
bash setup/setup_ubuntu.sh && source ~/.bashrc
bash setup/start_services.sh          # MongoDB + HBase + HBase Thrift server
```
(Optional alternative: `docker compose up -d mongodb hbase` starts the two databases in Docker.)

## 2. Quick start (everything)
```bash
python run_all.py                     # SCALE 0.1: 200k sessions / 50k transactions (~10-20 min)
python run_all.py --scale 1.0         # full brief size: 2M sessions / 500k transactions
python run_all.py --skip-db           # Spark + charts only, no databases
python run_all.py --from spark        # resume from a step
```

## 3. Step by step
| Step | Command | Output |
|---|---|---|
| Generate data | `cd data_generation && SCALE=0.1 OUTPUT_DIR=../data/raw python dataset_generator.py` | `data/raw/*.json` |
| Start databases | `bash setup/start_services.sh` | MongoDB :27017, HBase Thrift :9090, UI :16010 |
| MongoDB | `cd mongodb && python load_mongodb.py && python aggregations.py --explain` | `data/output/mongo_*.csv` |
| HBase schema | `cd hbase && python create_schema.py` (or `hbase shell hbase/create_schema.hbase`) | 2 tables |
| HBase load/query | `cd hbase && python load_hbase.py && python query_hbase.py --user user_000042` | console |
| HBase shell demo | `hbase shell hbase/sample_queries.hbase` | console |
| Spark cleaning | `cd spark && python 01_clean_normalize.py` | `data/processed/*` Parquet + `data_quality_report.json` |
| Recommendations | `python 02_recommendations.py` | `rec_*.csv` |
| Cohorts | `python 03_cohort_analysis.py` | `cohort_*.csv` |
| Spark SQL | `python 04_spark_sql_analytics.py` | `sql_*.csv` |
| Integration | `cd integration && python clv_analysis.py --source db` and `python funnel_analysis.py --source db` | `integration_*.csv` |
| Figures | `python visualization/make_charts.py` | `report/figures/fig1..fig8.png` |
| Dashboard | `python visualization/build_dashboard.py` | `report/dashboard.html` (open in any browser) |

`--source files` runs the integrated queries from Parquet (no databases); `--source db` reads users
and transactions from MongoDB and sessions from HBase — the analytics code is identical.

## 4. Useful mongosh queries
```js
use ecommerce
db.users.findOne({_id: "user_000042"})                               // profile + embedded purchase_summary
db.products.find({"category.category_id": "cat_007", base_price: {$lt: 50}}).limit(5)
db.transactions.find({user_id: "user_000042"}).sort({timestamp: -1}).limit(3)   // uses {user_id, timestamp} index
db.transactions.find({"items.product_id": "prod_00123"}).count()      // multikey index
```

## 5. Configuration
All paths and connection strings are in `config.py` and can be overridden with environment variables
(`MONGO_URI`, `HBASE_HOST`, `HBASE_PORT`, `RAW_DIR`, `END_DATE`, `HBASE_SESSION_FILES`, `SPARK_DRIVER_MEMORY`).

## 6. Notes
* The supplied `dataset_generator.py` is a Colab notebook saved with a `.py` name; the original code is
  extracted unchanged in `data_generation/dataset_generator_original.py`. The improved generator keeps the same
  model and output format but fixes reproducibility, adds `subcategory_id`, streams sessions to disk and
  takes a `SCALE` factor (see its docstring).
* Numbers in the report come from a `SCALE=0.1` run with `END_DATE=2026-10-01`.
* Troubleshooting: see the table at the end of SETUP_GUIDE.md.
