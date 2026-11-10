"""
Add execution screenshots to the technical report as an appendix.

Put this file in:      C:\\ecommerce-analytics\\report\\add_screenshots.py
Put screenshots in:    C:\\ecommerce-analytics\\report\\screenshots\\  (names listed below)
Run (from project):    python report/add_screenshots.py

Output:
  report/Appendix_Screenshots.pdf               (appendix only)
  report/Technical_Report_with_Screenshots.pdf  (report + appendix, needs pypdf)

Run it AFTER build_report.py, because build_report.py rewrites Technical_Report.pdf.
Missing screenshot files are skipped with a warning, so you can add them gradually.
"""
import os
import sys

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.lib.utils import ImageReader
from reportlab.platypus import (Image, KeepTogether, PageBreak, Paragraph,
                                SimpleDocTemplate, Spacer)

HERE = os.path.dirname(os.path.abspath(__file__))
SHOTS = os.path.join(HERE, "screenshots")
REPORT = os.path.join(HERE, "Technical_Report.pdf")
APPENDIX = os.path.join(HERE, "Appendix_Screenshots.pdf")
FINAL = os.path.join(HERE, "Technical_Report_with_Screenshots.pdf")

# (section heading) or (file name, figure title, caption)
SCREENSHOTS = [
    "A.1 Environment and services",
    ("01_environment.png", "Ubuntu (WSL) environment and project folder",
     "The project runs in Ubuntu on WSL with the bda-venv Python environment active; ls shows the "
     "pipeline folders (data_generation, mongodb, hbase, spark, integration, visualization, report)."),
    ("02_start_services.png", "Java 17 check and starting MongoDB and HBase",
     "java -version confirms OpenJDK 17 for Spark; setup/start_services.sh starts MongoDB (0.25 GB cache), "
     "the HBase master and the HBase Thrift server on port 9090."),
    ("03_hbase_ui_region_server.png", "HBase Web UI - region server",
     "HBase 2.5.16 standalone region server at localhost:16010, serving 4 regions."),
    ("04_hbase_ui_tables.png", "HBase Web UI - user tables",
     "Both tables created by hbase/create_schema.py are ENABLED: product_metrics_daily (family m, TTL 400 days, "
     "GZ) and user_sessions (families s IN_MEMORY, pv, c, all GZ)."),

    "A.2 Data generation and MongoDB (Part 1)",
    ("05_generate_and_mongo_load.png", "Dataset generation and MongoDB load",
     "run_all.py generates 25 categories, 5,000 products, 10,000 users, 100,000 sessions and 25,000 transactions "
     "(SCALE = 0.05) and loads them into MongoDB with a computed purchase_summary per user."),
    ("06_mongo_revenue_by_category.png", "MongoDB aggregation - revenue by category and month",
     "$match, $project (pro-rata discount), $unwind, $group, $lookup; --explain prints the winning plan."),
    ("07_mongo_product_popularity.png", "MongoDB aggregation - product popularity ($facet)",
     "Top products by units, revenue and distinct buyers in one round-trip; 4,775 products have at least one sale."),
    ("08_mongo_rfm_segments.png", "MongoDB aggregation - RFM segmentation and $bucket",
     "Seven customer segments with revenue share and recency, followed by the order-frequency $bucket pipeline."),

    "A.3 HBase (Part 1)",
    ("09_hbase_load_and_queries.png", "HBase schema, load and queries Q1-Q3",
     "Tables recreated, 10,000 sessions loaded (representative subset), 78,559 product_metrics_daily rows; "
     "Q1 latest sessions, Q2 date-window scan, Q3 full session journey with cart."),
    ("10_hbase_queries_q4_q6.png", "HBase queries Q4-Q6",
     "Q4/Q5 product daily metrics and weekly view-to-purchase roll-up; Q6 atomic counter increment (reverted). "
     "HBase is then stopped to free memory for Spark."),

    "A.4 Spark processing (Part 2)",
    ("11_spark_clean_quality_report.png", "Job 01 - cleaning, Parquet star schema and data-quality report",
     "999,527 page views, 103,939 cart items, 25,000 transactions and 55,199 order lines written; no mismatches "
     "or orphans, 7,740 'converted' sessions without a transaction."),
    ("12_spark_recommendations.png", "Job 02 - bought-together and viewed-together pairs",
     "9,216 baskets give 1,639 product pairs with at least 2 co-buyers; support, confidence and lift computed."),
    ("13_spark_affinity_fpgrowth_cohorts.png", "Job 02/03 - category affinity, FP-Growth and registration cohorts",
     "Category lift of 1.19-1.23 (near independence); FP-Growth finds no frequent multi-item sets; "
     "registration cohorts by activity month."),
    ("14_spark_cohort_matrices.png", "Job 03 - acquisition-cohort retention and revenue matrices",
     "56-58% of each first-purchase cohort buys again the next month; median 18 days to the second order."),
    ("15_spark_sql_physical_plan.png", "Job 04 - Spark SQL physical plan",
     "Parquet scans with pushed-down filters and a BroadcastHashJoin of the product dimension."),
    ("16_spark_sql_category_performance.png", "Job 04 - category revenue, profit and ranks",
     "Window functions rank categories by net revenue and gross profit; margins range from 18% to 36%."),
    ("17_spark_sql_daily_revenue_conversion.png", "Job 04 - daily revenue trend and conversion by device x referrer",
     "7-day moving average and week-over-week change; conversion of 12.0-13.8% across all device/referrer pairs."),
    ("18_spark_sql_views_vs_sales.png", "Job 04 - product views vs cart adds vs units sold",
     "Per-product funnel with view and sales ranks."),
    ("19_spark_sql_cart_price_crossstore.png", "Job 04 - abandoned carts, price changes and cross-store query",
     "Abandoned-cart value by category, sales after price moves, and engagement vs value by country "
     "(joining MongoDB and HBase views)."),

    "A.5 Integrated analytics and outputs (Parts 3-4)",
    ("20_integration_reconciliation_clv_start.png", "Reconciliation query and CLV retention estimate",
     "38% of 'converted' sessions have no order in both periods; HBase restarted and clv_analysis.py --source db "
     "estimates monthly retention r = 0.571."),
    ("21_integration_clv_results.png", "Customer lifetime value results",
     "CLV quintiles (top 20% hold 44% of 12-month CLV), engagement x value quadrants and the summary row "
     "(total CLV $5.26 M); funnel analysis starts."),
    ("22_integration_funnel_and_charts.png", "Conversion funnel, reconciliation and chart generation",
     "Funnel by device and referrer, reconciliation of tracking status against paid orders, and the eight report "
     "figures saved by make_charts.py."),
]

MAX_W = A4[0] - 3.6 * cm
MAX_H = 10.0 * cm


def scaled_image(path):
    w, h = ImageReader(path).getSize()
    ratio = min(MAX_W / w, MAX_H / h)
    img = Image(path, width=w * ratio, height=h * ratio)
    img.hAlign = "CENTER"
    return img


def build_appendix():
    styles = getSampleStyleSheet()
    title = ParagraphStyle("t", parent=styles["Heading1"],
                           textColor=colors.HexColor("#1f5fbf"), fontSize=16)
    section = ParagraphStyle("s", parent=styles["Heading2"], fontSize=12, spaceBefore=8)
    fig_title = ParagraphStyle("ft", parent=styles["Normal"], fontName="Helvetica-Bold",
                               fontSize=9.5, spaceBefore=4)
    caption = ParagraphStyle("c", parent=styles["Normal"], fontSize=8.5,
                             textColor=colors.HexColor("#444444"), leading=11, spaceAfter=10)

    story = [
        Paragraph("Appendix A - Execution Evidence (Screenshots)", title),
        Paragraph("Screenshots of the full pipeline (python run_all.py) running on the "
                  "development laptop: Ubuntu on WSL, 2.3 GB RAM, low-memory mode, "
                  "SCALE = 0.05 (100,000 sessions, 25,000 orders). The figures quoted in the main "
                  "report come from the SCALE = 0.1 run, so counts in these screenshots are about half "
                  "as large; the pipeline and code are identical. Source code: "
                  "https://github.com/hitayezujames/ecommerce-analytics", styles["Normal"]),
        Spacer(1, 6),
    ]

    n, missing = 0, []
    for entry in SCREENSHOTS:
        if isinstance(entry, str):
            story.append(Paragraph(entry, section))
            continue
        fname, ftitle, fcaption = entry
        path = os.path.join(SHOTS, fname)
        if not os.path.exists(path):
            missing.append(fname)
            continue
        n += 1
        story.append(KeepTogether([
            Paragraph(f"Figure A{n} - {ftitle}", fig_title),
            Spacer(1, 3),
            scaled_image(path),
            Spacer(1, 4),
            Paragraph(fcaption, caption),
        ]))

    if n == 0:
        sys.exit(f"No screenshots found in {SHOTS} - save them there first (see names in this script).")

    def footer(canvas, doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 7.5)
        canvas.setFillColor(colors.grey)
        canvas.drawString(1.8 * cm, 1.2 * cm,
                          "AUCA Big Data Analytics - Final Project: Appendix A")
        canvas.drawRightString(A4[0] - 1.8 * cm, 1.2 * cm, f"Appendix page {doc.page}")
        canvas.restoreState()

    doc = SimpleDocTemplate(APPENDIX, pagesize=A4, leftMargin=1.8 * cm, rightMargin=1.8 * cm,
                            topMargin=1.6 * cm, bottomMargin=1.8 * cm,
                            title="Appendix A - Execution Evidence")
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    print(f"wrote {APPENDIX}  ({n} screenshots)")
    for m in missing:
        print(f"  skipped (not found): screenshots/{m}")


def merge():
    try:
        from pypdf import PdfWriter
    except ImportError:
        print("pypdf not installed - run:  python -m pip install pypdf   then run this script again.")
        print("(The appendix PDF above is still usable on its own.)")
        return
    if not os.path.exists(REPORT):
        print(f"{REPORT} not found - run python report/build_report.py first.")
        return
    writer = PdfWriter()
    writer.append(REPORT)
    writer.append(APPENDIX)
    with open(FINAL, "wb") as f:
        writer.write(f)
    print(f"wrote {FINAL}")


if __name__ == "__main__":
    build_appendix()
    merge()
