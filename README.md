# Commerce Metrics and Self-Service Reporting

A reproducible commerce analytics and self-service BI pipeline. It ingests a transaction workbook, validates a typed schema, executes a PySpark pipeline, reconciles financial totals to an independent Python Decimal calculation, and produces a filterable dashboard, customer cohorts and a monthly business review.

## Run

Use Python 3.12 and Java 17 or 21. Install `requirements.txt`, set `JAVA_HOME` to the Java installation, then run:

```sh
python pipeline.py
python -m unittest -v
open results/dashboard.html
```

The pipeline downloads the public workbook on first run. Raw data, Parquet and SQLite remain local and are excluded from Git. CSV/JSON aggregate results and the standalone dashboard are committed for inspection. The Spark execution uses `local[2]` to execute the Spark SQL API and partitioned Parquet workflow reproducibly.

## Architecture

UCI workbook → validated row-preserving bronze CSV → typed PySpark facts → month-partitioned silver Parquet → monthly/country gold mart and customer cohort table → SQLite SQL, standalone dashboard and monthly review.

The workflow runs locally without external cloud credentials. Re-run boundary tests and source-to-mart reconciliation when updating monetary definitions, ingestion rules or cohort eligibility.

## Executed results

The full run processed 541,909 source lines into 314 month/country groups and 91 cohort cells across 13 observed months. All 13 reconciliation checks and seven boundary tests passed. Source hash, exact totals and Spark execution settings are recorded in `results/validation.json`; the dashboard's month/country filters and partial-month indicator were verified in Chrome.

## Data and scope

Primary: [UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail), Daqing Chen (2015), DOI 10.24432/C5BW33, CC BY 4.0. It contains historical UK retailer invoice lines. The downloaded artifact hash and observed counts are recorded in `results/validation.json`.

Backup: [UCI Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii), Daqing Chen (2012), DOI 10.24432/C5CG6D, CC BY 4.0. A separate ingestion adapter maps its `Invoice`, `Price` and `Customer ID` columns and prevents window overlap duplication.

## Metric definitions

Money is GBP. Each line is rounded half-up to integer pence before aggregation. Net recorded sales equal gross positive line value less absolute negative line value. Negative-price records are quarantined from monetary totals; zero-price records stay. Invoice identifiers beginning with C mark cancellations. Orders require positive quantity and a non-cancellation invoice. Missing customer IDs remain in financial totals and are excluded from cohorts. Exact repeated source lines are preserved to maintain full source ledger fidelity.

Retention is distinct active known buyers in a month divided by the original first-purchase cohort size. December 2011 ends on December 9 and is flagged in complete-month comparisons.

See `docs/schema.md`, `docs/training.md`, `self_service.sql` and the executed artifacts in `results/`.
