"""
build_dashboard.py - Part 4: interactive analytics dashboard (one self-contained HTML file).

Reads the result files written by the MongoDB, Spark and integration jobs (data/output/*.csv)
and writes report/dashboard.html. The page needs no server and no internet: open it in any
browser (double-click, or `explorer.exe report/dashboard.html` from WSL).

Usage:  python visualization/build_dashboard.py
"""
import json
import math
import os
import sys
from datetime import datetime

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config  # noqa: E402

OUT_HTML = os.path.join(config.ROOT, "report", "dashboard.html")


def load(name):
    path = os.path.join(config.OUTPUT_DIR, f"{name}.csv")
    if not os.path.exists(path):
        print(f"  (missing {name}.csv - that panel will be hidden)")
        return None
    return pd.read_csv(path)


def records(df, cols=None):
    if df is None:
        return None
    if cols:
        df = df[[c for c in cols if c in df.columns]]
    out = []
    for row in df.to_dict(orient="records"):
        out.append({k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row.items()})
    return out


def main():
    daily = load("sql_daily_revenue_trend")
    cats = load("sql_category_performance")
    funnel = load("integration_funnel_overall")
    by_dev = load("integration_funnel_by_device_type")
    by_ref = load("integration_funnel_by_referrer")
    segs = load("mongo_customer_segments")
    cohort = load("cohort_retention_matrix")
    tiers = load("integration_clv_tiers")
    quads = load("integration_clv_quadrants")
    clv = load("integration_clv_summary")
    aband = load("sql_abandoned_cart_value")
    prods = load("sql_product_view_vs_sales")
    recon = load("integration_funnel_reconciliation")

    if daily is not None:   # drop partial first/last days (a few hours of data)
        daily = daily[daily["orders"] >= daily["orders"].median() * 0.2].sort_values("txn_date")
    if prods is not None:
        prods["is_active"] = prods["is_active"].astype(str).str.lower() == "true"

    kpi = {}
    if daily is not None:
        kpi["revenue"] = float(daily["revenue"].sum())
        kpi["orders"] = int(daily["orders"].sum())
        kpi["aov"] = kpi["revenue"] / max(kpi["orders"], 1)
        kpi["days"] = int(len(daily))
        kpi["date_from"], kpi["date_to"] = str(daily["txn_date"].min()), str(daily["txn_date"].max())
    if funnel is not None:
        kpi["sessions"] = int(funnel["sessions"].iloc[0])
        kpi["conversion"] = float(funnel["sessions"].iloc[-1] / funnel["sessions"].iloc[0] * 100)
    if segs is not None:
        kpi["customers"] = int(segs["customers"].sum())
    if cats is not None:
        kpi["gross_profit"] = float(cats["gross_profit"].sum())
        kpi["margin"] = kpi["gross_profit"] / float(cats["net_revenue"].sum()) * 100
    if clv is not None:
        kpi["clv_total"] = float(clv["total_clv_12m"].iloc[0])
        kpi["top20"] = float(clv["top20pct_share_of_clv"].iloc[0])
        kpi["retention"] = float(clv["monthly_retention"].iloc[0]) * 100
    if aband is not None:
        kpi["abandoned"] = float(aband["abandoned_value"].sum())
    if prods is not None:
        act = prods[prods["is_active"]]
        kpi["sold_out"] = int((act["current_stock"] == 0).sum()) if "current_stock" in act else 0
    if recon is not None:
        conv = recon[recon["conversion_status"] == "converted"]
        if len(conv):
            kpi["converted_no_order"] = int(conv["without_order"].iloc[0])
            kpi["converted_sessions"] = int(conv["sessions"].iloc[0])

    dq_path = os.path.join(config.OUTPUT_DIR, "data_quality_report.json")
    dq = json.load(open(dq_path)) if os.path.exists(dq_path) else None

    data = {
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "kpi": kpi,
        "daily": records(daily, ["txn_date", "orders", "revenue", "revenue_7d_ma"]),
        "categories": records(cats, ["category_name", "net_revenue", "gross_profit", "margin_pct", "orders"]),
        "funnel": records(funnel),
        "by_device": records(by_dev, ["device_type", "s1", "s2", "s3", "s4", "s5"]),
        "by_referrer": records(by_ref, ["referrer", "s1", "s2", "s3", "s4", "s5"]),
        "segments": records(segs, ["segment", "customers", "avg_orders", "avg_spent", "revenue_share_pct",
                                   "avg_recency_days"]),
        "cohort": records(cohort),
        "tiers": records(tiers, ["value_tier", "users", "avg_clv_12m", "share_of_clv_pct", "avg_orders"]),
        "quadrants": records(quads, ["quadrant", "users", "avg_clv_12m", "avg_sessions", "avg_orders"]),
        "abandoned": records(aband, ["category_name", "abandoned_value", "abandoned_sessions"]),
        "products": records(prods, ["product_name", "category_name", "current_price", "current_stock",
                                    "is_active", "views", "cart_adds", "units_sold", "revenue"]),
        "dq": dq,
    }
    html = TEMPLATE.replace("__DATA__", json.dumps(data, default=str).replace("</", "<\\/"))
    os.makedirs(os.path.dirname(OUT_HTML), exist_ok=True)
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"wrote {OUT_HTML}  ({os.path.getsize(OUT_HTML) / 1024:.0f} KB)")


TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>E-commerce Analytics Dashboard</title>
<style>
:root{
  --bg:#eef2f7;--surface:#ffffff;--border:#e3e8ef;--grid:#edf0f4;--shadow:0 1px 2px rgba(16,24,40,.05),0 4px 14px rgba(16,24,40,.06);
  --s5:#e87ba4;--s7:#6250d6;
  --ink:#0b0b0b;--ink2:#52514e;--ink3:#7d7b76;
  --s1:#2a78d6;--s2:#eb6834;--s3:#1baf7a;--s4:#eda100;
  --seq0:#eef4fc;--seq1:#9cc0ee;--seq2:#2a78d6;--seq3:#123a6b;
  --good:#1a7f37;--warn:#b45309;
}
@media (prefers-color-scheme: dark){
  :root:not([data-theme="light"]){
    --bg:#0e1116;--surface:#171b22;--border:#262c36;--grid:#222832;--shadow:0 1px 2px rgba(0,0,0,.4);--s5:#d55181;--s7:#9085e9;
    --ink:#f0efec;--ink2:#c3c2b7;--ink3:#8f8d86;
    --s1:#3987e5;--s2:#d95926;--s3:#199e70;--s4:#c98500;
    --seq0:#1d2633;--seq1:#24497a;--seq2:#3987e5;--seq3:#a9cbf5;
    --good:#4ac26b;--warn:#e3a008;
  }
}
:root[data-theme="dark"]{
  --bg:#0e1116;--surface:#171b22;--border:#262c36;--grid:#222832;--shadow:0 1px 2px rgba(0,0,0,.4);--s5:#d55181;--s7:#9085e9;
  --ink:#f0efec;--ink2:#c3c2b7;--ink3:#8f8d86;
  --s1:#3987e5;--s2:#d95926;--s3:#199e70;--s4:#c98500;
  --seq0:#1d2633;--seq1:#24497a;--seq2:#3987e5;--seq3:#a9cbf5;
  --good:#4ac26b;--warn:#e3a008;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 "Segoe UI",system-ui,-apple-system,Roboto,sans-serif}
.banner{background:linear-gradient(120deg,#0b2350 0%,#1d5fae 55%,#178f69 100%);color:#fff;padding:26px 0 70px}
header{display:flex;flex-wrap:wrap;gap:12px;align-items:center;justify-content:space-between;padding:0 28px;max-width:1400px;margin:0 auto}
.brand{display:flex;align-items:center;gap:14px}
.logo{width:44px;height:44px;border-radius:12px;background:rgba(255,255,255,.16);display:grid;place-items:center}
h1{font-size:24px;margin:0;letter-spacing:-.01em;color:#fff}
.sub{color:rgba(255,255,255,.82);font-size:13px;margin-top:3px}
.chips{display:flex;gap:6px;margin-top:8px;flex-wrap:wrap}
.chip{background:rgba(255,255,255,.14);border:1px solid rgba(255,255,255,.22);color:#fff;border-radius:99px;padding:2px 10px;font-size:11.5px}
.tbtn{border:1px solid rgba(255,255,255,.35);background:rgba(255,255,255,.12);color:#fff;border-radius:10px;padding:8px 14px;cursor:pointer;font:inherit;font-size:13px}
.tbtn:hover{background:rgba(255,255,255,.22)}
main{max-width:1400px;margin:-52px auto 0;padding:0 28px 40px;display:grid;grid-template-columns:repeat(12,1fr);gap:18px}
.card{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:18px 20px;min-width:0;box-shadow:var(--shadow)}
.card h2{font-size:15.5px;margin:0 0 2px;display:flex;align-items:center;gap:8px}
.card h2::before{content:"";width:4px;height:16px;border-radius:2px;background:var(--accent,var(--s1))}
.card .note{color:var(--ink2);font-size:12.5px;margin:0 0 12px}
.span3{grid-column:span 3}.span4{grid-column:span 4}.span5{grid-column:span 5}.span6{grid-column:span 6}.span7{grid-column:span 7}.span8{grid-column:span 8}.span12{grid-column:span 12}
.kpis{grid-column:span 12;display:grid;grid-template-columns:repeat(4,1fr);gap:18px}
@media (max-width:900px){.kpis{grid-template-columns:repeat(2,1fr)}}
.kpi{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:16px 18px;box-shadow:var(--shadow);position:relative;overflow:hidden;display:flex;gap:14px;align-items:flex-start}
.kpi::after{content:"";position:absolute;left:0;right:0;top:0;height:4px;background:var(--c)}
.kpi .ic{flex:none;width:40px;height:40px;border-radius:11px;display:grid;place-items:center;background:color-mix(in srgb,var(--c) 15%,transparent);color:var(--c)}
.kpi .ic svg{width:21px;height:21px}
.kpi .l{color:var(--ink2);font-size:11.5px;text-transform:uppercase;letter-spacing:.05em;font-weight:600}
.kpi .v{font-size:26px;font-weight:700;margin-top:2px;font-variant-numeric:tabular-nums;letter-spacing:-.01em}
.kpi .d{color:var(--ink3);font-size:12px;margin-top:1px}
.kpi.alert .v{color:var(--warn)}
.seg{display:inline-flex;border:1px solid var(--border);border-radius:8px;overflow:hidden;margin-bottom:10px}
.seg button{border:0;background:transparent;color:var(--ink2);padding:5px 11px;font:inherit;font-size:12.5px;cursor:pointer}
.seg button.on{background:var(--s1);color:#fff}
.seg{background:var(--bg)}
.legend{display:flex;flex-wrap:wrap;gap:14px;font-size:12px;color:var(--ink2);margin-bottom:6px}
.legend i{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:5px;vertical-align:-1px}
svg{display:block;width:100%;height:auto;overflow:visible}
svg text{fill:var(--ink2);font-size:11px;font-family:inherit}
svg .lbl{fill:var(--ink);font-size:11.5px}
.grid line{stroke:var(--grid)}
.hit{fill:transparent;cursor:crosshair}
#tip{position:fixed;pointer-events:none;background:var(--ink);color:var(--surface);padding:7px 10px;border-radius:8px;font-size:12px;line-height:1.4;opacity:0;transition:opacity .08s;z-index:10;max-width:280px}
#tip b{font-weight:650}
table{width:100%;border-collapse:collapse;font-size:12.5px;font-variant-numeric:tabular-nums}
th,td{padding:6px 8px;border-bottom:1px solid var(--border);text-align:right;white-space:nowrap}
th:first-child,td:first-child,th:nth-child(2),td:nth-child(2){text-align:left}
th{color:var(--ink2);font-weight:600;cursor:pointer;user-select:none;position:sticky;top:0;background:var(--surface);text-transform:uppercase;font-size:11px;letter-spacing:.04em}
tbody tr:hover td{background:color-mix(in srgb,var(--s1) 7%,transparent)}
th.sorted::after{content:" \2193"}th.sorted.asc::after{content:" \2191"}
.tablewrap{max-height:420px;overflow:auto}
.dbar{display:flex;align-items:center;justify-content:flex-end;gap:8px}
.dbar span.b{display:inline-block;height:8px;border-radius:4px;background:var(--s3);opacity:.85}
.pill{display:inline-block;padding:1px 7px;border-radius:99px;font-size:11px;border:1px solid var(--border);color:var(--ink2)}
.pill.out{color:var(--warn);border-color:var(--warn)}
input[type=search]{border:1px solid var(--border);background:var(--bg);color:var(--ink);border-radius:8px;padding:6px 10px;font:inherit;font-size:13px;width:220px;max-width:100%}
.toolbar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;justify-content:space-between;margin-bottom:8px}
.heat{display:grid;gap:4px;font-size:12.5px}
.heat div{padding:10px 6px;border-radius:6px;text-align:center;font-variant-numeric:tabular-nums}
.heat .h{color:var(--ink2);padding:4px}
.insights{margin:0;padding-left:18px;color:var(--ink);font-size:13px}
.insights li{margin:6px 0}
.dq{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:6px 18px;font-size:12.5px}
.dq div{display:flex;justify-content:space-between;border-bottom:1px dashed var(--border);padding:4px 0}
.dq span:last-child{font-variant-numeric:tabular-nums;font-weight:600}
footer{max-width:1400px;margin:0 auto;padding:0 28px 30px;color:var(--ink3);font-size:12px}
@media (max-width:1000px){.span3,.span4,.span5,.span6,.span7,.span8{grid-column:span 12}}
@media (max-width:600px){header,main,footer{padding-left:16px;padding-right:16px}h1{font-size:20px}}
</style>
</head>
<body>
<div class="banner"><header>
  <div class="brand">
    <div class="logo"><svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2" stroke-linecap="round"><path d="M4 20V10M10 20V4M16 20v-7M22 20H2"/></svg></div>
    <div><h1>E-commerce Analytics Dashboard</h1><div class="sub" id="subtitle"></div>
      <div class="chips"><span class="chip">MongoDB</span><span class="chip">HBase</span><span class="chip">Apache Spark</span></div></div>
  </div>
  <button class="tbtn" id="themeBtn" type="button">Dark / light mode</button>
</header></div>
<main>
  <section class="kpis" id="kpis"></section>

  <div class="card span8" id="c-daily" style="--accent:var(--s1)"><h2>Daily revenue</h2><p class="note">Spark SQL query 2 &middot; hover for daily values</p>
    <div class="legend"><span><i style="background:var(--s1);opacity:.4"></i>Daily revenue</span><span><i style="background:var(--s1)"></i>7-day moving average</span></div>
    <div id="daily"></div></div>

  <div class="card span4" id="c-insights" style="--accent:var(--s7)"><h2>Key insights</h2><p class="note">Computed from the result files</p><ul class="insights" id="insights"></ul></div>

  <div class="card span7" id="c-funnel" style="--accent:var(--s1)"><h2>Conversion funnel</h2><p class="note">HBase sessions (stages 1-4) joined to MongoDB orders (stage 5)</p>
    <div class="seg" id="funnelSeg"></div><div id="funnel"></div></div>

  <div class="card span5" id="c-seg" style="--accent:var(--s2)"><h2>Customer segments (RFM)</h2><p class="note">MongoDB aggregation pipeline &middot; share of customers vs share of revenue</p>
    <div class="legend"><span><i style="background:var(--s1)"></i>% of customers</span><span><i style="background:var(--s2)"></i>% of revenue</span></div>
    <div id="segments"></div></div>

  <div class="card span7" id="c-cat" style="--accent:var(--s3)"><h2>Category performance</h2><p class="note">Spark SQL query 1 &middot; sorted by the selected measure</p>
    <div class="seg" id="catSeg"></div><div id="cats"></div></div>

  <div class="card span5" id="c-ab" style="--accent:var(--s4)"><h2>Abandoned-cart value by category</h2><p class="note">Spark SQL query 5 &middot; value left in carts that never became orders</p><div id="aband"></div></div>

  <div class="card span4" id="c-tiers" style="--accent:var(--s1)"><h2>12-month CLV by quintile</h2><p class="note">Integration query (MongoDB + HBase + Spark)</p><div id="tiers"></div></div>

  <div class="card span4" id="c-quad" style="--accent:var(--s3)"><h2>Engagement &times; value</h2><p class="note">Average CLV per quadrant</p><div id="quads"></div></div>

  <div class="card span4" id="c-cohort" style="--accent:var(--s7)"><h2>Repeat-purchase retention</h2><p class="note">Spark cohort job &middot; % of each first-purchase cohort buying again</p><div id="cohort"></div></div>

  <div class="card span7" id="c-scatter" style="--accent:var(--s1)"><h2>Views vs. units sold</h2><p class="note">One dot per product &middot; hover for details, click a dot to find it in the table</p>
    <div class="legend"><span><i style="background:var(--s1)"></i>In stock</span><span><i style="background:var(--s2)"></i>Sold out</span><span><i style="background:var(--ink3)"></i>Inactive</span></div>
    <div id="scatter"></div><p class="note" id="scatterNote" style="margin:8px 0 0"></p></div>

  <div class="card span5" id="c-top" style="--accent:var(--s3)"><h2>Top 10 products by revenue</h2><p class="note">Hover for views, cart adds and stock</p><div id="topProducts"></div></div>

  <div class="card span12" id="c-prod" style="--accent:var(--s5)"><div class="toolbar"><div><h2>Products</h2><p class="note" style="margin:0">Spark SQL query 4 &middot; click a column to sort</p></div>
    <input type="search" id="prodSearch" placeholder="Search product or category"></div>
    <div class="tablewrap"><table id="prodTable"></table></div></div>

  <div class="card span12" id="c-dq" style="--accent:var(--s3)"><h2>Data quality report</h2><p class="note">From the Spark cleaning job</p><div class="dq" id="dq"></div></div>
</main>
<footer id="foot"></footer>
<div id="tip"></div>
<script>
const D = __DATA__;
const $ = s => document.querySelector(s);
const NS = "http://www.w3.org/2000/svg";
const fmtMoney = v => v == null ? "-" : (Math.abs(v) >= 1e6 ? "$" + (v/1e6).toFixed(2) + "M" : Math.abs(v) >= 1e5 ? "$" + (v/1e3).toFixed(0) + "k" : Math.abs(v) >= 1e3 ? "$" + (v/1e3).toFixed(1) + "k" : "$" + v.toFixed(0));
const fmtInt = v => v == null ? "-" : Math.round(v).toLocaleString("en-US");
const fmtPct = (v, d=1) => v == null ? "-" : v.toFixed(d) + "%";
const css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();

/* ---------- theme ---------- */
try { const t = localStorage.getItem("dash-theme"); if (t) document.documentElement.dataset.theme = t; } catch (e) {}
$("#themeBtn").onclick = () => {
  const dark = document.documentElement.dataset.theme ? document.documentElement.dataset.theme === "dark"
             : matchMedia("(prefers-color-scheme: dark)").matches;
  document.documentElement.dataset.theme = dark ? "light" : "dark";
  try { localStorage.setItem("dash-theme", document.documentElement.dataset.theme); } catch (e) {}
  renderAll();
};

/* ---------- tooltip ---------- */
const tip = $("#tip");
function showTip(e, html){ tip.innerHTML = html; tip.style.opacity = 1;
  const x = Math.min(e.clientX + 14, innerWidth - tip.offsetWidth - 8), y = Math.max(8, e.clientY - tip.offsetHeight - 10);
  tip.style.left = x + "px"; tip.style.top = y + "px"; }
const hideTip = () => tip.style.opacity = 0;

function el(tag, attrs={}, parent){ const n = document.createElementNS(NS, tag);
  for (const k in attrs) n.setAttribute(k, attrs[k]); if (parent) parent.appendChild(n); return n; }
function svgBox(container, w, h){ container.innerHTML = ""; return el("svg", {viewBox:`0 0 ${w} ${h}`, width:w, height:h, role:"img"}, container); }
function widthOf(container, fallback){ return Math.max(280, container.clientWidth || fallback); }
function axis(v){ const raw = (v || 1) / 4, p = Math.pow(10, Math.floor(Math.log10(raw)));
  const step = [1, 2, 2.5, 5, 10].map(m => m*p).find(x => x >= raw); const n = Math.ceil(v/step - 1e-9) || 1; return {max: step*n, step, n}; }
function niceMax(v){ const p = Math.pow(10, Math.floor(Math.log10(v || 1))); for (const m of [1,1.5,2,2.5,3,4,5,6,8,10]) if (m*p >= v) return m*p; return 10*p; }
function roundTopRect(x, y, w, h, r){ r = Math.min(r, w/2, h); return `M${x},${y+h}V${y+r}Q${x},${y} ${x+r},${y}H${x+w-r}Q${x+w},${y} ${x+w},${y+r}V${y+h}Z`; }
function roundRightRect(x, y, w, h, r){ r = Math.min(r, h/2, w); return `M${x},${y}H${x+w-r}Q${x+w},${y} ${x+w},${y+r}V${y+h-r}Q${x+w},${y+h} ${x+w-r},${y+h}H${x}Z`; }

/* ---------- horizontal bar chart (one or two series) ---------- */
function hbars(container, rows, opts){
  const W = widthOf(container, 640), lw = Math.min(opts.labelWidth || 170, W*.42), rowH = opts.rowH || 26, top = 4, n = rows.length;
  const series = opts.series, H = top + n*rowH + 22;
  const svg = svgBox(container, W, H), plotW = W - lw - (opts.rightPad || 70);
  const ax = axis(Math.max(...rows.flatMap(r => series.map(s => r[s.key] || 0)))), max = ax.max;
  const g = el("g", {class:"grid"}, svg);
  for (let i = 0; i <= ax.n; i++){ const x = lw + plotW*i/ax.n;
    el("line", {x1:x, x2:x, y1:top, y2:H-20}, g);
    el("text", {x, y:H-6, "text-anchor":"middle"}, svg).textContent = opts.fmt(ax.step*i); }
  rows.forEach((r, i) => {
    const y0 = top + i*rowH, bandH = rowH - 6, bh = series.length > 1 ? (bandH-2)/series.length : bandH;
    el("text", {x:lw-8, y:y0+rowH/2+1, "text-anchor":"end", class:"lbl"}, svg).textContent = r[opts.label];
    series.forEach((s, j) => {
      const w = Math.max(0, (r[s.key] || 0) / max * plotW), y = y0 + 3 + j*(bh+2);
      const fill = opts.shade ? opts.shade(i, n) : css(s.color);
      el("path", {d:roundRightRect(lw, y, w, bh, 4), fill}, svg);
      if (series.length === 1 && opts.valueLabel) el("text", {x:lw+w+5, y:y+bh/2+4}, svg).textContent = opts.valueLabel(r);
    });
    const hit = el("rect", {x:0, y:y0, width:W, height:rowH, class:"hit"}, svg);
    hit.addEventListener("mousemove", e => showTip(e, opts.tip(r)));
    hit.addEventListener("mouseleave", hideTip);
  });
}

/* ---------- KPI tiles ---------- */
const ICON = {
  money:'<path d="M12 2v20M17 6H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/>',
  bag:'<path d="M6 7h12l1 14H5L6 7zM9 7a3 3 0 0 1 6 0"/>',
  trend:'<path d="M3 17l6-6 4 4 8-8M15 7h6v6"/>',
  eye:'<path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12z"/><circle cx="12" cy="12" r="3"/>',
  users:'<circle cx="9" cy="8" r="4"/><path d="M2 21a7 7 0 0 1 14 0M17 4a4 4 0 0 1 0 8M22 21a6 6 0 0 0-4-5.6"/>',
  star:'<path d="M12 3l2.8 5.8 6.2.9-4.5 4.4 1 6.2L12 17.4 6.5 20.3l1-6.2L3 9.7l6.2-.9z"/>',
  cart:'<circle cx="9" cy="20" r="1.5"/><circle cx="18" cy="20" r="1.5"/><path d="M2 3h3l2.5 12h11.5l2-8H6.2"/>',
  box:'<path d="M21 8l-9-5-9 5v8l9 5 9-5V8zM3 8l9 5 9-5M12 13v8"/>'};
const icon = n => `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${ICON[n]}</svg>`;
function renderKpis(){
  const k = D.kpi, tiles = [];
  if (k.revenue != null) tiles.push(["Revenue", fmtMoney(k.revenue), `${fmtInt(k.orders)} orders over ${k.days} days`, 0, "money", "--s1"]);
  if (k.aov != null) tiles.push(["Avg order value", "$" + fmtInt(k.aov), "all channels", 0, "bag", "--s7"]);
  if (k.gross_profit != null) tiles.push(["Gross profit", fmtMoney(k.gross_profit), fmtPct(k.margin) + " margin", 0, "trend", "--s3"]);
  if (k.sessions != null) tiles.push(["Sessions", fmtInt(k.sessions), fmtPct(k.conversion, 2) + " became paid orders", 0, "eye", "--s1"]);
  if (k.customers != null) tiles.push(["Customers", fmtInt(k.customers), "with at least one order", 0, "users", "--s5"]);
  if (k.clv_total != null) tiles.push(["12-month CLV", fmtMoney(k.clv_total), "top 20% hold " + fmtPct(k.top20), 0, "star", "--s3"]);
  if (k.abandoned != null) tiles.push(["Abandoned carts", fmtMoney(k.abandoned), "value never checked out", 1, "cart", "--s2"]);
  if (k.sold_out != null) tiles.push(["Sold-out products", fmtInt(k.sold_out), "active but zero stock", k.sold_out > 0, "box", "--s4"]);
  $("#kpis").innerHTML = tiles.map(t => `<div class="kpi${t[3] ? " alert" : ""}" style="--c:var(${t[5]})"><div class="ic">${icon(t[4])}</div><div><div class="l">${t[0]}</div><div class="v">${t[1]}</div><div class="d">${t[2]}</div></div></div>`).join("");
}

/* ---------- daily revenue line ---------- */
function renderDaily(){
  const d = D.daily; if (!d || !d.length){ $("#c-daily").style.display = "none"; return; }
  const W = widthOf($("#daily"), 860), H = 280, L = 58, R = 12, T = 10, B = 26, pw = W-L-R, ph = H-T-B;
  const ax = axis(Math.max(...d.map(r => r.revenue))), max = ax.max, svg = svgBox($("#daily"), W, H);
  const x = i => L + (d.length === 1 ? pw/2 : i/(d.length-1)*pw), y = v => T + ph - v/max*ph;
  const g = el("g", {class:"grid"}, svg);
  for (let i = 0; i <= ax.n; i++){ const yy = T + ph - ph*i/ax.n; el("line", {x1:L, x2:W-R, y1:yy, y2:yy}, g);
    el("text", {x:L-8, y:yy+4, "text-anchor":"end"}, svg).textContent = fmtMoney(ax.step*i); }
  const step = Math.ceil(d.length/Math.max(3, Math.floor(pw/90)));
  d.forEach((r, i) => { if (i % step === 0) el("text", {x:x(i), y:H-6, "text-anchor":"middle"}, svg).textContent = r.txn_date.slice(5); });
  const path = key => d.map((r, i) => (i ? "L" : "M") + x(i).toFixed(1) + "," + y(r[key]).toFixed(1)).join("");
  el("path", {d:path("revenue"), fill:"none", stroke:css("--s1"), "stroke-opacity":.4, "stroke-width":1.5}, svg);
  el("path", {d:path("revenue_7d_ma"), fill:"none", stroke:css("--s1"), "stroke-width":2.5}, svg);
  const cross = el("line", {y1:T, y2:T+ph, stroke:css("--ink3"), "stroke-dasharray":"3 3", opacity:0}, svg);
  const dot = el("circle", {r:4.5, fill:css("--s1"), stroke:css("--surface"), "stroke-width":2, opacity:0}, svg);
  const hit = el("rect", {x:L, y:T, width:pw, height:ph, class:"hit"}, svg);
  hit.addEventListener("mousemove", e => {
    const b = svg.getBoundingClientRect(), mx = (e.clientX - b.left) * W / b.width;
    const i = Math.max(0, Math.min(d.length-1, Math.round((mx-L)/pw*(d.length-1)))), r = d[i];
    cross.setAttribute("x1", x(i)); cross.setAttribute("x2", x(i)); cross.setAttribute("opacity", 1);
    dot.setAttribute("cx", x(i)); dot.setAttribute("cy", y(r.revenue)); dot.setAttribute("opacity", 1);
    showTip(e, `<b>${r.txn_date}</b><br>Revenue ${fmtMoney(r.revenue)}<br>7-day avg ${fmtMoney(r.revenue_7d_ma)}<br>${fmtInt(r.orders)} orders`);
  });
  hit.addEventListener("mouseleave", () => { cross.setAttribute("opacity", 0); dot.setAttribute("opacity", 0); hideTip(); });
}

/* ---------- funnel ---------- */
const STAGES = ["Session", "Product view", "Add to cart", "Checkout", "Paid order"];
let funnelView = "all";
function renderFunnel(){
  if (!D.funnel){ $("#c-funnel").style.display = "none"; return; }
  const views = [["all", "Overall"]]; if (D.by_device) views.push(["device", "By device"]); if (D.by_referrer) views.push(["referrer", "By referrer"]);
  $("#funnelSeg").innerHTML = views.map(v => `<button type="button" data-v="${v[0]}" class="${v[0] === funnelView ? "on" : ""}">${v[1]}</button>`).join("");
  $("#funnelSeg").querySelectorAll("button").forEach(b => b.onclick = () => { funnelView = b.dataset.v; renderFunnel(); });
  const box = $("#funnel");
  if (funnelView === "all"){
    const rows = D.funnel.map((r, i) => ({label: STAGES[i] || r.stage, v: r.sessions, prev: i ? D.funnel[i-1].sessions : null}));
    const first = rows[0].v;
    hbars(box, rows, {label:"label", series:[{key:"v", color:"--s1"}], labelWidth:110, rowH:40, rightPad:150,
      shade: (i, n) => i === n - 1 ? css("--s3") : css("--s1"),   // paid orders highlighted in green
      fmt: v => v >= 1000 ? (v/1000).toFixed(0) + "k" : v.toFixed(0),
      valueLabel: r => `${fmtInt(r.v)}  (${(r.v/first*100).toFixed(1)}%)`,
      tip: r => `<b>${r.label}</b><br>${fmtInt(r.v)} sessions<br>${(r.v/first*100).toFixed(1)}% of all sessions` + (r.prev ? `<br>${(r.v/r.prev*100).toFixed(1)}% of previous step` : "")});
  } else {
    const src = funnelView === "device" ? D.by_device : D.by_referrer, key = funnelView === "device" ? "device_type" : "referrer";
    const rows = src.map(r => ({label:r[key], conv:r.s5/r.s1*100, cart:r.s3/r.s1*100, chk:r.s4/r.s1*100, n:r.s1, s5:r.s5}))
                    .sort((a, b) => b.conv - a.conv);
    box.innerHTML = '<div class="legend"><span><i style="background:var(--s1)"></i>Added to cart %</span><span><i style="background:var(--s3)"></i>Reached checkout %</span><span><i style="background:var(--s2)"></i>Paid order %</span></div><div></div>';
    hbars(box.lastChild, rows, {label:"label", labelWidth:110, rowH:44,
      series:[{key:"cart", color:"--s1"}, {key:"chk", color:"--s3"}, {key:"conv", color:"--s2"}],
      fmt: v => v.toFixed(0) + "%",
      tip: r => { const ci = 1.96*Math.sqrt(r.conv/100*(1-r.conv/100)/r.n)*100;
        return `<b>${r.label}</b><br>${fmtInt(r.n)} sessions<br>Add to cart ${r.cart.toFixed(1)}%<br>Checkout ${r.chk.toFixed(1)}%<br>Paid order ${r.conv.toFixed(2)}% (&plusmn;${ci.toFixed(2)} pp, 95% CI)`; }});
  }
}

/* ---------- segments (grouped vertical bars) ---------- */
function renderSegments(){
  const s = D.segments; if (!s){ $("#c-seg").style.display = "none"; return; }
  const tot = s.reduce((a, r) => a + r.customers, 0);
  const rows = s.map(r => ({...r, name: String(r.segment).replace(/^\d+\s*/, ""), cust: r.customers/tot*100}));
  const W = widthOf($("#segments"), 560), H = 290, L = 36, B = 64, T = 8, pw = W-L-8, ph = H-T-B, n = rows.length;
  const ax = axis(Math.max(...rows.map(r => Math.max(r.cust, r.revenue_share_pct)))), max = ax.max, svg = svgBox($("#segments"), W, H);
  const g = el("g", {class:"grid"}, svg);
  for (let i = 0; i <= ax.n; i++){ const yy = T + ph - ph*i/ax.n; el("line", {x1:L, x2:W-8, y1:yy, y2:yy}, g);
    el("text", {x:L-6, y:yy+4, "text-anchor":"end"}, svg).textContent = (ax.step*i).toFixed(0) + "%"; }
  const band = pw/n, bw = Math.min(26, (band-10)/2);
  rows.forEach((r, i) => {
    const cx = L + band*i + band/2;
    [[r.cust, "--s1", -1], [r.revenue_share_pct, "--s2", 1]].forEach(([v, c, side]) => {
      const h = v/max*ph, x = side < 0 ? cx - bw - 1 : cx + 1;
      el("path", {d:roundTopRect(x, T+ph-h, bw, h, 3), fill:css(c)}, svg); });
    const words = r.name.split(" "), mid = Math.ceil(words.length/2);
    [words.slice(0, mid).join(" "), words.slice(mid).join(" ")].forEach((line, k) => {
      if (line) el("text", {x:cx, y:T+ph+15+k*13, "text-anchor":"middle"}, svg).textContent = line; });
    const hit = el("rect", {x:cx-band/2, y:T, width:band, height:ph+40, class:"hit"}, svg);
    hit.addEventListener("mousemove", e => showTip(e, `<b>${r.name}</b><br>${fmtInt(r.customers)} customers (${r.cust.toFixed(1)}%)<br>${fmtPct(r.revenue_share_pct)} of revenue<br>Avg spend $${fmtInt(r.avg_spent)} &middot; ${(+r.avg_orders).toFixed(1)} orders<br>Last order ${(+r.avg_recency_days).toFixed(0)} days ago (avg)`));
    hit.addEventListener("mouseleave", hideTip);
  });
}

/* ---------- categories ---------- */
let catMetric = "net_revenue";
function renderCats(){
  const c = D.categories; if (!c){ $("#c-cat").style.display = "none"; return; }
  const ms = [["net_revenue", "Revenue"], ["gross_profit", "Gross profit"], ["margin_pct", "Margin %"]];
  $("#catSeg").innerHTML = ms.map(m => `<button type="button" data-m="${m[0]}" class="${m[0] === catMetric ? "on" : ""}">${m[1]}</button>`).join("");
  $("#catSeg").querySelectorAll("button").forEach(b => b.onclick = () => { catMetric = b.dataset.m; renderCats(); });
  const rows = [...c].sort((a, b) => b[catMetric] - a[catMetric]);
  const isPct = catMetric === "margin_pct";
  hbars($("#cats"), rows, {label:"category_name", labelWidth:170, rowH:20, rightPad:60,
    series:[{key:catMetric, color: catMetric === "net_revenue" ? "--s1" : catMetric === "gross_profit" ? "--s2" : "--s3"}],
    fmt: v => isPct ? v.toFixed(0) + "%" : fmtMoney(v),
    valueLabel: r => isPct ? r.margin_pct.toFixed(1) + "%" : fmtMoney(r[catMetric]),
    tip: r => `<b>${r.category_name}</b><br>Revenue ${fmtMoney(r.net_revenue)}<br>Gross profit ${fmtMoney(r.gross_profit)}<br>Margin ${fmtPct(r.margin_pct)}<br>${fmtInt(r.orders)} orders`});
}

/* ---------- abandoned ---------- */
function renderAband(){
  const a = D.abandoned; if (!a){ $("#c-ab").style.display = "none"; return; }
  const rows = [...a].sort((x, y) => y.abandoned_value - x.abandoned_value).slice(0, 12);
  hbars($("#aband"), rows, {label:"category_name", labelWidth:180, rowH:24, rightPad:64, series:[{key:"abandoned_value", color:"--s4"}],
    fmt: fmtMoney, valueLabel: r => fmtMoney(r.abandoned_value),
    tip: r => `<b>${r.category_name}</b><br>${fmtMoney(r.abandoned_value)} left in carts<br>${fmtInt(r.abandoned_sessions)} abandoned sessions`});
  if (a.length > 12) $("#aband").insertAdjacentHTML("beforeend", `<p class="note" style="margin-top:6px">Top 12 of ${a.length} categories</p>`);
}

/* ---------- CLV tiers ---------- */
function renderTiers(){
  const t = D.tiers; if (!t){ $("#c-tiers").style.display = "none"; return; }
  const W = widthOf($("#tiers"), 420), H = 250, L = 34, T = 16, B = 26, pw = W-L-6, ph = H-T-B, n = t.length;
  const ax = axis(Math.max(...t.map(r => r.share_of_clv_pct)) * 1.1), max = ax.max, svg = svgBox($("#tiers"), W, H);
  const g = el("g", {class:"grid"}, svg);
  for (let i = 0; i <= ax.n; i++){ const yy = T + ph - ph*i/ax.n; el("line", {x1:L, x2:W-6, y1:yy, y2:yy}, g);
    el("text", {x:L-6, y:yy+4, "text-anchor":"end"}, svg).textContent = (ax.step*i).toFixed(0) + "%"; }
  const eq = T + ph - 100/n/max*ph;
  el("line", {x1:L, x2:W-6, y1:eq, y2:eq, stroke:css("--ink3"), "stroke-dasharray":"4 3"}, svg);
  el("text", {x:W-8, y:eq-5, "text-anchor":"end"}, svg).textContent = "equal share";
  const band = pw/n, bw = Math.min(46, band*.6);
  t.forEach((r, i) => { const cx = L + band*i + band/2, h = r.share_of_clv_pct/max*ph;
    el("path", {d:roundTopRect(cx-bw/2, T+ph-h, bw, h, 5), fill:[css("--seq3"), css("--seq2"), css("--seq2"), css("--seq1"), css("--seq1")][i] || css("--s1")}, svg);
    el("text", {x:cx, y:T+ph-h-5, "text-anchor":"middle", class:"lbl"}, svg).textContent = r.share_of_clv_pct.toFixed(1) + "%";
    el("text", {x:cx, y:H-8, "text-anchor":"middle"}, svg).textContent = r.value_tier;
    const hit = el("rect", {x:cx-band/2, y:T, width:band, height:ph, class:"hit"}, svg);
    hit.addEventListener("mousemove", e => showTip(e, `<b>${r.value_tier}</b> (${r.value_tier === "Q1" ? "top" : r.value_tier === "Q5" ? "bottom" : "middle"} 20%)<br>${fmtInt(r.users)} users<br>Avg CLV $${fmtInt(r.avg_clv_12m)}<br>${fmtPct(r.share_of_clv_pct)} of total CLV<br>${(+r.avg_orders).toFixed(1)} orders on average`));
    hit.addEventListener("mouseleave", hideTip); });
}

/* ---------- quadrants ---------- */
function renderQuads(){
  const q = D.quadrants; if (!q){ $("#c-quad").style.display = "none"; return; }
  const rows = [...q].sort((a, b) => b.avg_clv_12m - a.avg_clv_12m).map(r => ({...r, short: r.quadrant.replace(/\s*\(.*\)/, "")}));
  hbars($("#quads"), rows, {label:"short", labelWidth:130, rowH:44, rightPad:60, series:[{key:"avg_clv_12m", color:"--s3"}],
    fmt: v => "$" + fmtInt(v), valueLabel: r => "$" + fmtInt(r.avg_clv_12m),
    tip: r => `<b>${r.quadrant}</b><br>${fmtInt(r.users)} users<br>Avg CLV $${fmtInt(r.avg_clv_12m)}<br>${(+r.avg_sessions).toFixed(1)} sessions &middot; ${(+r.avg_orders).toFixed(1)} orders`});
}

/* ---------- cohort heatmap ---------- */
function renderCohort(){
  const c = D.cohort; if (!c){ $("#c-cohort").style.display = "none"; return; }
  const periods = Object.keys(c[0]).filter(k => !["first_month", "new_customers"].includes(k));
  const color = v => v == null ? "transparent" : v >= 90 ? css("--seq3") : v >= 60 ? css("--seq2") : v >= 30 ? css("--seq1") : css("--seq0");
  const ink = v => v != null && v >= 60 ? "#fff" : css("--ink");
  let html = `<div class="heat" style="grid-template-columns:110px repeat(${periods.length},1fr)"><div class="h"></div>` +
    periods.map(p => `<div class="h">Month ${p}</div>`).join("");
  c.forEach(r => { html += `<div class="h" style="text-align:left">${r.first_month}<br><small>n=${fmtInt(r.new_customers)}</small></div>`;
    periods.forEach(p => { const v = r[p];
      html += v == null ? `<div style="color:var(--ink3);border:1px dashed var(--border)">not yet</div>`
                        : `<div style="background:${color(v)};color:${ink(v)}">${(+v).toFixed(1)}%</div>`; }); });
  $("#cohort").innerHTML = html + "</div>";
}


/* ---------- scatter: views vs units sold ---------- */
function renderScatter(){
  const p = D.products; if (!p){ $("#c-scatter").style.display = "none"; return; }
  const box = $("#scatter"), W = widthOf(box, 760), H = 340, L = 46, R = 14, T = 10, B = 40, pw = W-L-R, ph = H-T-B;
  const svg = svgBox(box, W, H);
  const ax = axis(Math.max(...p.map(r => r.views || 0))), ay = axis(Math.max(...p.map(r => r.units_sold || 0)));
  const x = v => L + v/ax.max*pw, y = v => T + ph - v/ay.max*ph;
  const g = el("g", {class:"grid"}, svg);
  for (let i = 0; i <= ay.n; i++){ const yy = y(ay.step*i); el("line", {x1:L, x2:W-R, y1:yy, y2:yy}, g);
    el("text", {x:L-6, y:yy+4, "text-anchor":"end"}, svg).textContent = fmtInt(ay.step*i); }
  for (let i = 0; i <= ax.n; i++){ const xx = x(ax.step*i); el("line", {x1:xx, x2:xx, y1:T, y2:T+ph}, g);
    el("text", {x:xx, y:T+ph+16, "text-anchor":"middle"}, svg).textContent = fmtInt(ax.step*i); }
  el("text", {x:L+pw/2, y:H-4, "text-anchor":"middle"}, svg).textContent = "Product-detail views";
  const yl = el("text", {x:12, y:T+ph/2, "text-anchor":"middle", transform:`rotate(-90 12 ${T+ph/2})`}, svg); yl.textContent = "Units sold";
  const kind = r => !r.is_active ? "inactive" : r.current_stock === 0 ? "out" : "ok";
  const col = {ok:css("--s1"), out:css("--s2"), inactive:css("--ink3")};
  const order = {ok:0, inactive:1, out:2};   // draw sold-out on top so they stay visible
  [...p].sort((a, b) => order[kind(a)] - order[kind(b)]).forEach(r => {
    const k = kind(r);
    const c = el("circle", {cx:x(r.views || 0), cy:y(r.units_sold || 0), r:k === "ok" ? 3 : 4,
      fill:col[k], "fill-opacity":k === "ok" ? .45 : .9, stroke:css("--surface"), "stroke-width":k === "ok" ? 0 : 1}, svg);
    c.style.cursor = "pointer";
    c.addEventListener("mousemove", e => showTip(e, `<b>${r.product_name}</b><br>${r.category_name}<br>${fmtInt(r.views)} views &middot; ${fmtInt(r.units_sold)} sold<br>Revenue ${fmtMoney(r.revenue)}<br>${k === "inactive" ? "Inactive" : k === "out" ? "SOLD OUT" : fmtInt(r.current_stock) + " in stock"}`));
    c.addEventListener("mouseleave", hideTip);
    c.addEventListener("click", () => { $("#prodSearch").value = r.product_name; renderProducts();
      $("#c-prod").scrollIntoView({behavior:"smooth", block:"start"}); });
  });
  // correlation for in-stock vs all products
  const corr = rows => { const n = rows.length; if (n < 3) return null;
    const mx = rows.reduce((a, r) => a + r.views, 0)/n, my = rows.reduce((a, r) => a + r.units_sold, 0)/n;
    let sxy = 0, sxx = 0, syy = 0; rows.forEach(r => { const dx = r.views - mx, dy = r.units_sold - my; sxy += dx*dy; sxx += dx*dx; syy += dy*dy; });
    return sxy / Math.sqrt(sxx*syy); };
  const all = corr(p.filter(r => r.is_active)), inStock = corr(p.filter(r => r.is_active && r.current_stock > 0));
  const out = p.filter(r => r.is_active && r.current_stock === 0);
  if (all != null) $("#scatterNote").innerHTML = `Correlation views&ndash;sales: <b>${all.toFixed(2)}</b> for all active products, but only <b>${(inStock ?? 0).toFixed(2)}</b> for in-stock ones. `
    + (out.length ? `The <b>${out.length}</b> sold-out products (orange) sit bottom-left: once stock ran out they stopped being shown, so they collected fewer views and sales.` : "");
}

/* ---------- top 10 products ---------- */
function renderTop(){
  const p = D.products; if (!p){ $("#c-top").style.display = "none"; return; }
  const rows = [...p].sort((a, b) => b.revenue - a.revenue).slice(0, 10).map((r, i) => ({...r, lab: (i+1) + ". " + r.product_name}));
  hbars($("#topProducts"), rows, {label:"lab", labelWidth:210, rowH:30, rightPad:62, series:[{key:"revenue", color:"--s3"}],
    fmt: fmtMoney, valueLabel: r => fmtMoney(r.revenue),
    tip: r => `<b>${r.product_name}</b><br>${r.category_name}<br>Revenue ${fmtMoney(r.revenue)} &middot; ${fmtInt(r.units_sold)} units<br>${fmtInt(r.views)} views &middot; ${fmtInt(r.cart_adds)} cart adds<br>Price $${(+r.current_price).toFixed(2)} &middot; ${r.current_stock === 0 ? "SOLD OUT" : fmtInt(r.current_stock) + " in stock"}`});
}

/* ---------- products table ---------- */
let sortKey = "revenue", sortAsc = false;
const COLS = [["product_name", "Product"], ["category_name", "Category"], ["current_price", "Price"], ["views", "Views"],
              ["cart_adds", "Cart adds"], ["units_sold", "Units sold"], ["revenue", "Revenue"], ["current_stock", "Stock"]];
function renderProducts(){
  const p = D.products; if (!p){ $("#c-prod").style.display = "none"; return; }
  const q = ($("#prodSearch").value || "").toLowerCase();
  const rows = p.filter(r => !q || (r.product_name + " " + r.category_name).toLowerCase().includes(q))
    .sort((a, b) => { const x = a[sortKey], y = b[sortKey];
      const c = typeof x === "string" ? x.localeCompare(y) : (x || 0) - (y || 0); return sortAsc ? c : -c; }).slice(0, 200);
  const head = "<thead><tr>" + COLS.map(([k, l]) => `<th data-k="${k}" class="${k === sortKey ? "sorted" + (sortAsc ? " asc" : "") : ""}">${l}</th>`).join("") + "</tr></thead>";
  const maxRev = Math.max(...p.map(r => r.revenue || 0)) || 1;
  const body = rows.map(r => `<tr><td>${r.product_name}</td><td>${r.category_name}</td><td>$${(+r.current_price).toFixed(2)}</td><td>${fmtInt(r.views)}</td><td>${fmtInt(r.cart_adds)}</td><td>${fmtInt(r.units_sold)}</td><td><div class="dbar"><span>${fmtMoney(r.revenue)}</span><span class="b" style="width:${Math.max(2, (r.revenue || 0)/maxRev*70).toFixed(0)}px"></span></div></td><td>${
      !r.is_active ? '<span class="pill">inactive</span>' : r.current_stock === 0 ? '<span class="pill out">sold out</span>' : fmtInt(r.current_stock)}</td></tr>`).join("");
  $("#prodTable").innerHTML = head + "<tbody>" + body + "</tbody>";
  $("#prodTable").querySelectorAll("th").forEach(th => th.onclick = () => {
    if (sortKey === th.dataset.k) sortAsc = !sortAsc; else { sortKey = th.dataset.k; sortAsc = th.dataset.k.endsWith("name"); }
    renderProducts(); });
}
$("#prodSearch").addEventListener("input", renderProducts);

/* ---------- insights ---------- */
function renderInsights(){
  const k = D.kpi, out = [];
  if (k.abandoned != null && D.funnel){
    out.push(`<b>${fmtMoney(k.abandoned)}</b> is sitting in abandoned carts &mdash; the biggest recoverable pool of revenue.`); }
  if (D.funnel && D.funnel.length >= 5){
    const f = D.funnel.map(r => r.sessions), steps = f.slice(1).map((v, i) => v/f[i]), worst = steps.indexOf(Math.min(...steps));
    out.push(`Weakest funnel step: <b>${STAGES[worst]} &rarr; ${STAGES[worst+1]}</b> (${(steps[worst]*100).toFixed(0)}% continue).`); }
  if (k.sold_out) out.push(`<b>${k.sold_out}</b> active products are sold out and get far fewer views &mdash; restock alerts needed.`);
  if (D.categories){ const c = [...D.categories].sort((a, b) => b.margin_pct - a.margin_pct);
    out.push(`Margins range from <b>${c[c.length-1].margin_pct.toFixed(0)}%</b> to <b>${c[0].margin_pct.toFixed(0)}%</b>; ${c[0].category_name} is the most profitable per dollar.`); }
  if (D.segments){ const top = D.segments.filter(r => /^[12]/.test(String(r.segment)));
    if (top.length) { const tot = D.segments.reduce((a, r) => a + r.customers, 0);
      out.push(`Champions + Loyal: <b>${(top.reduce((a, r) => a + r.customers, 0)/tot*100).toFixed(0)}%</b> of customers, <b>${top.reduce((a, r) => a + r.revenue_share_pct, 0).toFixed(0)}%</b> of revenue.`); } }
  if (k.top20 != null) out.push(`Top 20% of customers hold <b>${fmtPct(k.top20)}</b> of 12-month CLV (monthly retention ${fmtPct(k.retention)}).`);
  if (k.converted_no_order != null && k.converted_sessions)
    out.push(`<b>${fmtInt(k.converted_no_order)}</b> of ${fmtInt(k.converted_sessions)} "converted" sessions have no matching order (tracking vs. order store).`);
  $("#insights").innerHTML = out.map(t => `<li>${t}</li>`).join("") || "<li>Run the pipeline to generate insights.</li>";
}

/* ---------- data quality ---------- */
function renderDq(){
  const d = D.dq; if (!d){ $("#c-dq").style.display = "none"; return; }
  const flat = []; (function walk(o, p){ for (const k in o){ const v = o[k];
    if (v && typeof v === "object") walk(v, p + k + " / "); else flat.push([p + k.replace(/_/g, " "), v]); } })(d, "");
  $("#dq").innerHTML = flat.map(([k, v]) => `<div><span>${k}</span><span>${typeof v === "number" ? fmtInt(v) : v}</span></div>`).join("");
}

function renderAll(){
  const k = D.kpi;
  $("#subtitle").textContent = (k.date_from ? `Data ${k.date_from} to ${k.date_to} · ` : "") + "MongoDB + HBase + Apache Spark";
  $("#foot").textContent = "Generated " + D.generated + " by visualization/build_dashboard.py from data/output/*.csv";
  renderKpis(); renderDaily(); renderInsights(); renderFunnel(); renderSegments(); renderCats(); renderAband();
  renderTiers(); renderQuads(); renderCohort(); renderScatter(); renderTop(); renderProducts(); renderDq();
}
renderAll();
let rt; addEventListener("resize", () => { clearTimeout(rt); rt = setTimeout(renderAll, 150); });
</script>
</body>
</html>
"""

if __name__ == "__main__":
    main()
