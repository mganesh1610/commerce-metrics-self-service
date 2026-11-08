"""Public transaction analytics with explicit financial and cohort definitions."""
from __future__ import annotations

import argparse
import calendar
import csv
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import html
import json
from pathlib import Path
import sqlite3
import urllib.request
import zipfile

from openpyxl import load_workbook
from pyspark.sql import SparkSession, functions as F, types as T

SOURCE = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"
DOC = "https://archive.ics.uci.edu/dataset/352/online+retail"
EXPECTED_SHA256 = "f5385cbb54bbebf7196389109c6b0621faab0c304e3702548165e71c84aede8b"
HEADERS = ["row_id", "invoice_no", "stock_code", "description", "quantity", "invoice_at", "unit_price", "customer_id", "country"]


def line_minor(quantity, unit_price):
    price = Decimal(str(unit_price))
    if price < 0:
        return None
    return int((Decimal(quantity) * price * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def ratio(numerator, denominator):
    return numerator / denominator if denominator else None


def prepare(data_dir):
    data_dir.mkdir(parents=True, exist_ok=True)
    archive = data_dir / "online_retail.zip"
    if not archive.exists():
        urllib.request.urlretrieve(SOURCE, archive)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    if digest != EXPECTED_SHA256:
        raise ValueError("Source artifact changed; review provenance before accepting a new hash")
    workbook_path = data_dir / "Online Retail.xlsx"
    if not workbook_path.exists():
        with zipfile.ZipFile(archive) as z:
            with z.open("Online Retail.xlsx") as src, workbook_path.open("wb") as dst:
                dst.write(src.read())
    wb = load_workbook(workbook_path, read_only=True, data_only=True)
    ws = wb.active
    rows = ws.iter_rows(values_only=True)
    actual = next(rows)
    expected = ("InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country")
    if actual != expected:
        raise ValueError(f"Unexpected workbook schema: {actual}")
    csv_path = data_dir / "bronze.csv"
    totals = {"rows": 0, "net_minor": 0, "gross_minor": 0, "returns_minor": 0, "negative_price_rows": 0, "missing_customer_rows": 0}
    dates = []
    with csv_path.open("w", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(HEADERS)
        for i, row in enumerate(rows, 1):
            invoice, stock, desc, qty, at, price, customer, country = row
            if not isinstance(at, datetime) or not isinstance(qty, (int, float)):
                raise ValueError(f"Malformed input row {i}")
            customer = "" if customer is None else str(int(customer))
            writer.writerow([i, str(invoice), str(stock), desc or "", int(qty), at.isoformat(), str(price), customer, country])
            totals["rows"] += 1
            totals["missing_customer_rows"] += not bool(customer)
            minor = line_minor(int(qty), price)
            if minor is None:
                totals["negative_price_rows"] += 1
            else:
                totals["net_minor"] += minor
                totals["gross_minor"] += max(minor, 0)
                totals["returns_minor"] += max(-minor, 0)
            dates.append(at.date())
    wb.close()
    totals.update({"source_url": SOURCE, "source_documentation": DOC, "source_sha256": digest,
                   "date_min": str(min(dates)), "date_max": str(max(dates))})
    return csv_path, totals


def cohort_counts(rows):
    """Small independent reference used by boundary tests."""
    purchases = {}
    for customer, month, quantity in rows:
        if customer and quantity > 0:
            purchases.setdefault(customer, set()).add(month)
    out = {}
    for customer, months in purchases.items():
        first = min(months)
        for month in months:
            out[(first, month)] = out.get((first, month), 0) + 1
    return out


def run(data_dir, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    path, controls = prepare(data_dir)
    spark = (SparkSession.builder.master("local[2]").appName("commerce-metrics-self-service")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "Europe/London").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    schema = T.StructType([
        T.StructField("row_id", T.LongType(), False), T.StructField("invoice_no", T.StringType()),
        T.StructField("stock_code", T.StringType()), T.StructField("description", T.StringType()),
        T.StructField("quantity", T.IntegerType()), T.StructField("invoice_at", T.StringType()),
        T.StructField("unit_price", T.DecimalType(18, 6)), T.StructField("customer_id", T.StringType()),
        T.StructField("country", T.StringType())])
    bronze = spark.read.option("header", True).option("multiLine", True).schema(schema).csv(str(path))
    facts = (bronze.withColumn("invoice_at", F.to_timestamp("invoice_at"))
             .withColumn("month", F.date_format("invoice_at", "yyyy-MM"))
             .withColumn("is_cancellation", F.upper("invoice_no").startswith("C"))
             .withColumn("valid_price", F.col("unit_price") >= 0)
             .withColumn("amount_minor", F.when(F.col("valid_price"), F.round(F.col("quantity") * F.col("unit_price") * 100, 0).cast("long"))))
    facts.cache()
    facts.createOrReplaceTempView("invoice_lines")
    sql = """
    SELECT month, country, COUNT(*) AS source_lines,
      SUM(CASE WHEN valid_price THEN 1 ELSE 0 END) AS valid_lines,
      SUM(CASE WHEN valid_price THEN amount_minor ELSE 0 END) AS net_minor,
      SUM(CASE WHEN valid_price AND amount_minor>0 THEN amount_minor ELSE 0 END) AS gross_minor,
      SUM(CASE WHEN valid_price AND amount_minor<0 THEN -amount_minor ELSE 0 END) AS returns_minor,
      COUNT(DISTINCT CASE WHEN valid_price AND quantity>0 AND NOT is_cancellation THEN invoice_no END) AS orders,
      COUNT(DISTINCT CASE WHEN valid_price AND quantity>0 AND NOT is_cancellation THEN customer_id END) AS known_buyers
    FROM invoice_lines GROUP BY month, country ORDER BY month, country
    """
    mart = spark.sql(sql)
    max_date = datetime.fromisoformat(controls["date_max"])
    partial = max_date.strftime("%Y-%m") if max_date.day < calendar.monthrange(max_date.year, max_date.month)[1] else None
    metrics = [r.asDict() for r in mart.collect()]
    for r in metrics:
        r["return_value_rate"] = ratio(r["returns_minor"], r["gross_minor"])
        r["net_per_order_gbp"] = ratio(r["net_minor"] / 100, r["orders"])
        r["partial_month"] = r["month"] == partial
    buys = facts.filter(F.col("valid_price") & (F.col("quantity") > 0) & ~F.col("is_cancellation") & F.col("customer_id").isNotNull()).select("customer_id", "month").distinct()
    first = buys.groupBy("customer_id").agg(F.min("month").alias("cohort_month"))
    sizes = first.groupBy("cohort_month").count().withColumnRenamed("count", "cohort_size")
    cohorts_df = (buys.join(first, "customer_id").groupBy("cohort_month", "month").count()
                  .withColumnRenamed("count", "active_buyers").join(sizes, "cohort_month")
                  .withColumn("retention_rate", F.col("active_buyers") / F.col("cohort_size")).orderBy("cohort_month", "month"))
    cohorts = [r.asDict() for r in cohorts_df.collect()]
    spark_totals = spark.sql("SELECT COUNT(*) rows, SUM(amount_minor) net_minor, SUM(CASE WHEN amount_minor>0 THEN amount_minor ELSE 0 END) gross_minor, SUM(CASE WHEN amount_minor<0 THEN -amount_minor ELSE 0 END) returns_minor FROM invoice_lines").first().asDict()
    checks = {f"python_spark_{key}": controls[key] == spark_totals[key] for key in spark_totals}
    checks.update({"mart_line_count": sum(r["source_lines"] for r in metrics) == controls["rows"],
                   "mart_financial_reconciliation": sum(r["net_minor"] for r in metrics) == controls["net_minor"],
                   "gross_less_returns_equals_net": controls["gross_minor"] - controls["returns_minor"] == controls["net_minor"],
                   "unique_ingestion_keys": facts.select("row_id").distinct().count() == controls["rows"],
                   "cohort_rates_in_range": all(0 <= r["retention_rate"] <= 1 for r in cohorts),
                   "cohort_initial_rates": all(r["active_buyers"] == r["cohort_size"] for r in cohorts if r["cohort_month"] == r["month"]),
                   "missing_ids_retained_financials": facts.filter(F.col("customer_id").isNull()).count() == controls["missing_customer_rows"]})
    facts.write.mode("overwrite").partitionBy("month").parquet(str(data_dir / "silver"))
    mart.write.mode("overwrite").parquet(str(data_dir / "gold"))
    with sqlite3.connect(output_dir / "metrics.sqlite") as con:
        con.execute("DROP TABLE IF EXISTS monthly_metrics")
        con.execute("CREATE TABLE monthly_metrics(month TEXT,country TEXT,source_lines INT,valid_lines INT,net_minor INT,gross_minor INT,returns_minor INT,orders INT,known_buyers INT,return_value_rate REAL,net_per_order_gbp REAL,partial_month INT,PRIMARY KEY(month,country))")
        con.executemany("INSERT INTO monthly_metrics VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", [tuple(r.values()) for r in metrics])
        checks["sqlite_financial_parity"] = con.execute("SELECT SUM(net_minor) FROM monthly_metrics").fetchone()[0] == controls["net_minor"]
        checks["sqlite_integrity"] = con.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    for name, rows in [("monthly_metrics", metrics), ("cohort_retention", cohorts)]:
        with (output_dir / f"{name}.csv").open("w", newline="") as stream:
            w = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
            w.writeheader(); w.writerows(rows)
    (output_dir / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
    validation = {"source": controls, "execution": {"engine": spark.version, "master": "local[2]"},
                  "monthly_country_groups": len(metrics), "months": len({r["month"] for r in metrics}),
                  "cohort_cells": len(cohorts), "checks": checks, "passed": sum(checks.values()), "failed": sum(not v for v in checks.values()),
                  "partial_month": partial}
    (output_dir / "validation.json").write_text(json.dumps(validation, indent=2) + "\n")
    if not all(checks.values()):
        raise AssertionError(validation)
    build_dashboard(metrics, output_dir)
    build_brief(metrics, controls, output_dir)
    spark.stop()
    print(json.dumps(validation, indent=2))
    return validation


def build_brief(metrics, controls, output_dir):
    months = sorted({r["month"] for r in metrics if not r["partial_month"]})
    current, previous = months[-1], months[-2]
    totals = {m: sum(r["net_minor"] for r in metrics if r["month"] == m) / 100 for m in (current, previous)}
    growth = ratio(totals[current] - totals[previous], totals[previous])
    leaders = sorted((r for r in metrics if r["month"] == current), key=lambda r: r["net_minor"], reverse=True)[:5]
    text = f"# Monthly business review: {current}\n\nObserved data snapshot: {controls['date_min']} to {controls['date_max']}.\n\n"
    text += f"Net recorded sales: GBP {totals[current]:,.2f}; prior complete month GBP {totals[previous]:,.2f}; change {growth:.1%}.\n\n"
    text += "| Country | Net recorded sales (GBP) | Orders | Return value / gross positive sales |\n|---|---:|---:|---:|\n"
    for r in leaders:
        rate = f"{r['return_value_rate']:.1%}" if r["return_value_rate"] is not None else "unavailable"
        text += f"| {r['country']} | {r['net_minor']/100:,.2f} | {r['orders']:,} | {rate} |\n"
    text += "\nReview actions: inspect country concentration and return outliers; reconcile unusual lines before recommending budget or operational changes. Compare complete months only. December 2011 ends on December 9 and is excluded from the month-over-month comparison.\n\nMissing customer IDs remain in financial totals and are excluded from customer cohorts. Repeated source lines remain preserved to maintain source transaction fidelity. Negative-price lines are quarantined from financial totals.\n"
    (output_dir / "monthly_review.md").write_text(text)


def build_dashboard(metrics, output_dir):
    data = json.dumps(metrics).replace("</", "<\\/")
    page = """<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Commerce metrics</title><style>
    body{font:16px system-ui;color:#20272c;max-width:1080px;margin:48px auto;padding:0 24px;background:#f7f9fb}h1{color:#173b55}label{margin-right:24px}select{padding:8px;font:inherit}main{background:white;padding:24px;border:1px solid #dfe5eb;border-radius:12px}.cards{display:flex;gap:32px;margin:30px 0}.value{font-size:28px;font-weight:650}table{border-collapse:collapse;width:100%}td,th{padding:10px;text-align:left;border-bottom:1px solid #eee}td:nth-child(n+2){text-align:right}.note{color:#5e6871;line-height:1.5}#partial{color:#895e00}</style>
    <link rel="icon" href="data:,"><h1>Commerce metrics</h1><p class="note">UK retail ledger · GBP · invoice-line grain</p><main><label>Month <select id="month" aria-label="Month"></select></label><label>Country <select id="country" aria-label="Country"></select></label><p id="partial"></p><div class="cards"><div>Net recorded sales<div class="value" id="net"></div></div><div>Orders<div class="value" id="orders"></div></div><div>Return value rate<div class="value" id="returns"></div></div></div><table><thead><tr><th>Country</th><th>Net sales (GBP)</th><th>Orders</th><th>Source lines</th></tr></thead><tbody id="body"></tbody></table><p class="note">Returns reduce net sales. Negative-price records are excluded. Missing customer IDs remain in financial totals. Buyer counts are not additive across countries. Partial months must not be compared with full months.</p></main><p class="note">Use this view to locate a change, then inspect the SQL and source rows before making a recommendation.</p><script>
    const data=__DATA__;const month=document.querySelector('#month'),country=document.querySelector('#country');
    [...new Set(data.map(r=>r.month))].sort().forEach(x=>month.add(new Option(x,x)));month.value='2011-11';country.add(new Option('All countries',''));[...new Set(data.map(r=>r.country))].sort().forEach(x=>country.add(new Option(x,x)));
    function draw(){const rows=data.filter(r=>r.month===month.value&&(!country.value||r.country===country.value));const sum=k=>rows.reduce((n,r)=>n+r[k],0);document.querySelector('#net').textContent=(sum('net_minor')/100).toLocaleString('en-GB',{style:'currency',currency:'GBP'});document.querySelector('#orders').textContent=sum('orders').toLocaleString();document.querySelector('#returns').textContent=sum('gross_minor')?(100*sum('returns_minor')/sum('gross_minor')).toFixed(1)+'%':'Unavailable';document.querySelector('#partial').textContent=rows.some(r=>r.partial_month)?'Partial month: data ends on 9 December 2011.':'';const body=document.querySelector('#body');body.replaceChildren();rows.sort((a,b)=>b.net_minor-a.net_minor).forEach(r=>{const tr=document.createElement('tr');[r.country,(r.net_minor/100).toFixed(2),r.orders,r.source_lines].forEach(v=>{const td=document.createElement('td');td.textContent=v;tr.append(td)});body.append(tr)});}month.onchange=country.onchange=draw;draw();</script></html>"""
    (output_dir / "dashboard.html").write_text(page.replace("__DATA__", data))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--output-dir", type=Path, default=Path("results"))
    args = parser.parse_args()
    run(args.data_dir, args.output_dir)
