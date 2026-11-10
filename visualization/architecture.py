"""Draws report/figures/fig0_architecture.png (system architecture diagram)."""
import os, sys
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

INK, INK2, SURF = "#0b0b0b", "#52514e", "#fcfcfb"
C = {"src": "#eef0f2", "mongo": "#d9efe6", "hbase": "#fde4d8", "spark": "#dde9f8", "out": "#f3f0e0"}
fig, ax = plt.subplots(figsize=(11, 5.2)); fig.patch.set_facecolor(SURF); ax.set_facecolor(SURF)
ax.set_xlim(0, 110); ax.set_ylim(0, 58); ax.axis("off")

def box(x, y, w, h, title, body, color):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4,rounding_size=1.2", fc=color, ec="#9a9893", lw=1))
    ax.text(x + w / 2, y + h - 2.2, title, ha="center", va="top", fontsize=10, fontweight="bold", color=INK)
    ax.text(x + w / 2, y + h - 5.6, body, ha="center", va="top", fontsize=7.6, color=INK2, linespacing=1.35)

def arrow(x1, y1, x2, y2, label=""):
    ax.annotate("", (x2, y2), (x1, y1), arrowprops=dict(arrowstyle="-|>", color=INK2, lw=1.2, shrinkA=2, shrinkB=2))
    if label:
        ax.text((x1 + x2) / 2, (y1 + y2) / 2 + 1, label, fontsize=7, color=INK2, ha="center")

box(1, 15, 17, 22, "Data generator", "dataset_generator.py\nSCALE, fixed seed\nusers, products,\ncategories, sessions,\ntransactions (JSON)", C["src"])
box(26, 37, 22, 18, "MongoDB (documents)", "load_mongodb.py\nproducts (+category refs)\nusers (+purchase_summary)\ntransactions (+items)\n$jsonSchema, indexes, pipelines", C["mongo"])
box(26, 0.5, 22, 18, "HBase (wide-column)", "load_hbase.py\nuser_sessions: user|rev_ts|sid\nCF s / pv / c\nproduct_metrics_daily:\nproduct|yyyymmdd counters", C["hbase"])
box(26, 20, 22, 14, "Spark ETL (job 01)", "schema-on-read, clean,\ndedupe, explode nested,\nParquet star schema", C["spark"])
box(57, 20, 24, 35, "Apache Spark analytics", "02 recommendations\n(basket pairs, FP-Growth)\n03 cohort analysis\n04 Spark SQL (8 queries)\n\nIntegration (sources.py)\nCLV + engagement\nfunnel + reconciliation\n--source files | db", C["spark"])
box(57, 1, 24, 15, "Operational queries", "user timeline scans,\nsession journeys,\nproduct day metrics,\natomic counters", C["hbase"])
box(89, 20, 20, 35, "Insights", "result CSVs\nmatplotlib figures\ntechnical report\n\nbusiness actions:\nstock alerts, cart\nrecovery, CLV tiers", C["out"])
arrow(18, 33, 26, 44); arrow(18, 19, 26, 11); arrow(18, 27, 26, 27, "JSON")
arrow(48, 27, 57, 30, "Parquet"); arrow(48, 46, 57, 46, "pymongo"); arrow(48, 14, 57, 23)
arrow(48, 8, 57, 8, "happybase"); arrow(81, 37, 89, 37)
ax.text(1, 57, "System architecture: polyglot persistence with Spark as the integration layer", fontsize=12, fontweight="bold", color=INK)
fig.savefig(os.path.join(config.FIGURES_DIR, "fig0_architecture.png"), dpi=200, bbox_inches="tight")
print("saved")
