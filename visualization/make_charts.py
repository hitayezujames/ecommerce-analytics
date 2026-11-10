"""
make_charts.py - Part 4: static figures for the report (PNG, 200 dpi).

Reads the result CSVs written by the Spark / MongoDB / integration jobs in data/output/
and writes report/figures/fig*.png.  Every figure is produced only if its input exists,
so the script can be run after any subset of the pipeline.

Usage:  python make_charts.py
"""
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker as mtick  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config  # noqa: E402

# Validated categorical palette (fixed order) + text tokens
BLUE, ORANGE, AQUA, YELLOW = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
INK, INK2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
SEQ_BLUE = LinearSegmentedColormap.from_list("seq_blue", ["#eef4fc", "#9cc0ee", "#2a78d6", "#123a6b"])

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": "DejaVu Sans", "font.size": 10, "axes.titlesize": 12, "axes.titleweight": "bold",
    "axes.titlelocation": "left", "axes.edgecolor": GRID, "axes.labelcolor": INK2,
    "xtick.color": INK2, "ytick.color": INK2, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.8, "axes.spines.top": False, "axes.spines.right": False,
    "legend.frameon": False, "text.color": INK,
})


def src(name):
    p = os.path.join(config.OUTPUT_DIR, f"{name}.csv")
    return pd.read_csv(p) if os.path.exists(p) else None


def save(fig, name, note=None):
    if note:
        fig.text(0.01, 0.01, note, fontsize=8, color=INK2)
    out = os.path.join(config.FIGURES_DIR, f"{name}.png")
    fig.savefig(out, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("saved", out)


money = mtick.FuncFormatter(lambda v, _: f"${v / 1e6:.1f}M" if abs(v) >= 1e6 else f"${v / 1e3:.0f}k")


def fig_daily_revenue():
    d = src("sql_daily_revenue_trend")
    if d is None:
        return
    d["txn_date"] = pd.to_datetime(d["txn_date"])
    d = d[d["orders"] >= d["orders"].median() * 0.2]  # drop partial edge days
    fig, ax = plt.subplots(figsize=(9, 3.6))
    ax.plot(d["txn_date"], d["revenue"], color=BLUE, lw=1, alpha=0.45, label="Daily revenue")
    ax.plot(d["txn_date"], d["revenue_7d_ma"], color=BLUE, lw=2.2, label="7-day moving average")
    ax.yaxis.set_major_formatter(money)
    ax.set_ylim(0, d["revenue"].max() * 1.15)
    ax.set_title("Daily revenue is flat at the 7-day level - no trend or seasonality")
    ax.legend(loc="lower right")
    last = d.iloc[-1]
    ax.annotate(f"7-day avg {money(last['revenue_7d_ma'])}", (last["txn_date"], last["revenue_7d_ma"]),
                xytext=(-10, 18), textcoords="offset points", ha="right", fontsize=9, color=INK)
    save(fig, "fig1_daily_revenue")


def fig_category():
    c = src("sql_category_performance")
    if c is None:
        return
    c = c.sort_values("net_revenue")
    fig, ax = plt.subplots(figsize=(9, 7))
    y = np.arange(len(c))
    ax.barh(y + 0.2, c["net_revenue"], height=0.38, color=BLUE, label="Net revenue")
    ax.barh(y - 0.2, c["gross_profit"], height=0.38, color=ORANGE, label="Gross profit")
    ax.set_yticks(y, [f"{n}  ({m:.0f}% margin)" for n, m in zip(c["category_name"], c["margin_pct"])], fontsize=8)
    ax.xaxis.set_major_formatter(money)
    ax.grid(axis="y", visible=False)
    ax.set_title(f"Revenue is spread evenly across categories; profit is not "
                 f"(margins {c['margin_pct'].min():.0f}-{c['margin_pct'].max():.0f}%)")
    ax.legend(loc="lower right")
    save(fig, "fig2_category_revenue_profit")


def fig_funnel():
    f = src("integration_funnel_overall")
    if f is None:
        return
    fig, ax = plt.subplots(figsize=(9, 3.8))
    y = np.arange(len(f))[::-1]
    ax.barh(y, f["sessions"], color=BLUE, height=0.6)
    top = f["sessions"].iloc[0]
    for i, (yy, v) in enumerate(zip(y, f["sessions"])):
        step = "" if i == 0 else f" | {v / f['sessions'].iloc[i - 1]:.0%} of previous step"
        ax.text(v + top * 0.01, yy, f"{v:,.0f}  ({v / top:.1%} of sessions){step}", va="center", fontsize=9)
    ax.set_yticks(y, f["stage"])
    ax.set_xlim(0, top * 1.6)
    ax.grid(axis="y", visible=False)
    ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"{v / 1e3:.0f}k"))
    ax.set_title("Conversion funnel (HBase sessions joined to MongoDB orders)")
    save(fig, "fig3_conversion_funnel")


def fig_segments():
    s = src("mongo_customer_segments")
    if s is None:
        return
    s = s.sort_values("segment")
    labels = [x.split(" ", 1)[1] for x in s["segment"]]
    cust_share = s["customers"] / s["customers"].sum() * 100
    fig, ax = plt.subplots(figsize=(9, 4))
    x = np.arange(len(s))
    ax.bar(x - 0.2, cust_share, width=0.38, color=BLUE, label="% of customers")
    ax.bar(x + 0.2, s["revenue_share_pct"], width=0.38, color=ORANGE, label="% of revenue")
    for xi, v in zip(x, s["avg_spent"]):
        ax.text(xi, max(cust_share.max(), s["revenue_share_pct"].max()) * 1.05, f"${v:,.0f}\navg", ha="center",
                fontsize=8, color=INK2)
    ax.set_xticks(x, labels, rotation=15, ha="right", fontsize=9)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.set_ylim(0, max(cust_share.max(), s["revenue_share_pct"].max()) * 1.25)
    ax.grid(axis="x", visible=False)
    top2 = s["segment"].str.startswith(("1", "2"))
    ax.set_title(f"RFM segments: Champions + Loyal are {cust_share[top2].sum():.0f}% of customers "
                 f"and {s.loc[top2, 'revenue_share_pct'].sum():.0f}% of revenue")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.28), ncol=2)
    save(fig, "fig4_customer_segments")


def fig_cohort():
    r = src("cohort_retention_matrix")
    if r is None:
        return
    periods = [c for c in r.columns if c not in ("first_month", "new_customers")]
    m = r[periods].astype(float).values
    fig, ax = plt.subplots(figsize=(7, 2.8))
    ax.imshow(np.nan_to_num(m, nan=0), cmap=SEQ_BLUE, vmin=0, vmax=100, aspect="auto")
    for i in range(m.shape[0]):
        for j in range(m.shape[1]):
            if not np.isnan(m[i, j]):
                ax.text(j, i, f"{m[i, j]:.1f}%", ha="center", va="center", fontsize=10,
                        color="white" if m[i, j] > 55 else INK)
            else:
                ax.text(j, i, "not yet observed", ha="center", va="center", fontsize=8, color=INK2)
    ax.set_xticks(range(len(periods)), [f"Month {p}" for p in periods])
    ax.set_yticks(range(len(r)), [f"{a}  (n={b:,})" for a, b in zip(r["first_month"], r["new_customers"])])
    ax.grid(False)
    ax.set_title("Repeat-purchase retention by first-purchase cohort")
    save(fig, "fig5_cohort_retention")


def fig_views_vs_sales():
    p = src("sql_product_view_vs_sales")
    if p is None:
        return
    p["is_active"] = p["is_active"].astype(str).str.lower() == "true"
    fig, ax = plt.subplots(figsize=(7.5, 4.5))
    a, i = p[p["is_active"]], p[~p["is_active"]]
    ax.scatter(a["views"], a["units_sold"], s=9, color=BLUE, alpha=0.45, edgecolor="none", label=f"Active ({len(a):,})")
    ax.scatter(i["views"], i["units_sold"], s=14, color=ORANGE, alpha=0.9, edgecolor="none",
               label=f"Inactive ({len(i):,}) - never shown")
    r_all, r_act = p["views"].corr(p["units_sold"]), a["views"].corr(a["units_sold"])
    ax.set_xlabel("Product-detail views")
    ax.set_ylabel("Units sold")
    ax.set_title(f"Stock-outs, not popularity, drive the views-sales link (r={r_all:.2f}; in-stock only r={a[a.get('current_stock', 1) != 0]['views'].corr(a[a.get('current_stock', 1) != 0]['units_sold']):.2f})")
    so = a[a["current_stock"] == 0] if "current_stock" in a else a.iloc[0:0]
    if len(so):
        ax.scatter(so["views"], so["units_sold"], s=16, facecolor="none", edgecolor=INK, lw=0.8,
                   label=f"Sold out ({len(so):,}) - hidden from browsing once stock hit 0")
    ax.legend(loc="lower right", fontsize=8.5)
    save(fig, "fig6_views_vs_sales")


def fig_clv():
    t, q = src("integration_clv_tiers"), src("integration_clv_quadrants")
    if t is None or q is None:
        return
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.8), gridspec_kw={"width_ratios": [1, 1.3]})
    a1.bar(t["value_tier"], t["share_of_clv_pct"], color=BLUE, width=0.6)
    a1.axhline(20, color=INK2, lw=1, ls="--")
    a1.text(4.35, 21, "equal share (20%)", ha="right", fontsize=8, color=INK2)
    for x, v in enumerate(t["share_of_clv_pct"]):
        a1.text(x, v + 0.8, f"{v:.1f}%", ha="center", fontsize=9)
    a1.yaxis.set_major_formatter(mtick.PercentFormatter())
    a1.set_title("Share of 12-month CLV by quintile")
    a1.grid(axis="x", visible=False)
    q = q.sort_values("avg_clv_12m")
    a2.barh(q["quadrant"].str.replace(" (", "\n(", regex=False), q["avg_clv_12m"], color=AQUA, height=0.55)
    for y, (v, n) in enumerate(zip(q["avg_clv_12m"], q["users"])):
        a2.text(v * 1.01, y, f"${v:,.0f}  ({n:,} users)", va="center", fontsize=8)
    a2.set_xlim(0, q["avg_clv_12m"].max() * 1.45)
    a2.xaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"${v:,.0f}"))
    a2.set_title("Avg CLV by engagement x value quadrant")
    a2.grid(axis="y", visible=False)
    a2.tick_params(axis="y", labelsize=8)
    fig.tight_layout()
    save(fig, "fig7_clv_segments")


def fig_device_referrer():
    d, r = src("integration_funnel_by_device_type"), src("integration_funnel_by_referrer")
    if d is None or r is None:
        return
    fig, ax = plt.subplots(figsize=(9, 3.4))
    rows = [(f"device: {a}", b, c) for a, b, c in zip(d["device_type"], d["s5"], d["s1"])] + \
           [(f"referrer: {a}", b, c) for a, b, c in zip(r["referrer"], r["s5"], r["s1"])]
    labels = [x[0] for x in rows]
    p = np.array([x[1] / x[2] for x in rows])
    ci = 1.96 * np.sqrt(p * (1 - p) / np.array([x[2] for x in rows]))
    y = np.arange(len(rows))[::-1]
    ax.errorbar(p * 100, y, xerr=ci * 100, fmt="o", color=BLUE, ecolor=INK2, capsize=3, ms=6)
    ax.axvline(p.mean() * 100, color=ORANGE, lw=1.2, ls="--")
    ax.text(p.mean() * 100, -0.9, f" overall {p.mean():.1%}", color=INK2, fontsize=8)
    ax.set_ylim(-1.2, len(rows) - 0.5)
    ax.set_yticks(y, labels, fontsize=9)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(decimals=1))
    ax.set_title("Session-to-order conversion with 95% confidence intervals: no real differences")
    save(fig, "fig8_conversion_by_segment")


if __name__ == "__main__":
    for f in (fig_daily_revenue, fig_category, fig_funnel, fig_segments, fig_cohort,
              fig_views_vs_sales, fig_clv, fig_device_referrer):
        f()
