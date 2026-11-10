"""Central configuration shared by every script (override with environment variables)."""
import os

ROOT = os.path.dirname(os.path.abspath(__file__))

RAW_DIR = os.environ.get("RAW_DIR", os.path.join(ROOT, "data", "raw"))            # generator output
PROCESSED_DIR = os.environ.get("PROCESSED_DIR", os.path.join(ROOT, "data", "processed"))  # Spark parquet
OUTPUT_DIR = os.environ.get("ANALYTICS_OUTPUT_DIR", os.path.join(ROOT, "data", "output"))  # result CSVs
FIGURES_DIR = os.environ.get("FIGURES_DIR", os.path.join(ROOT, "report", "figures"))

# MongoDB
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "ecommerce")

# HBase (Thrift server, used by happybase)
HBASE_HOST = os.environ.get("HBASE_HOST", "localhost")
HBASE_PORT = int(os.environ.get("HBASE_PORT", "9090"))
HBASE_SESSIONS_TABLE = "user_sessions"
HBASE_PRODUCT_METRICS_TABLE = "product_metrics_daily"

# How many session files to load into HBase (each file = 100k sessions).
# "Significant representative subset" per the brief; set to 0 to load all.
HBASE_SESSION_FILES = int(os.environ.get("HBASE_SESSION_FILES", "2"))

# Analysis "today" = generator END_DATE, so recency metrics are reproducible
REFERENCE_DATE = os.environ.get("END_DATE", "2026-10-01")

for d in(PROCESSED_DIR, OUTPUT_DIR, FIGURES_DIR):
    os.makedirs(d, exist_ok=True)
