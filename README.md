# Commerce Metrics and Self-Service Reporting

A reproducible public-data analytics product built with Codex. It ingests a transaction workbook, validates a typed schema, executes a PySpark pipeline, reconciles financial totals to an independent Python Decimal calculation, and produces a filterable dashboard, customer cohorts and a monthly business review.

## Run

Use Python 3.12 and Java 17 or 21. Install `requirements.txt`, set `JAVA_HOME` to the Java installation, then run:

```sh
python pipeline.py
python -m unittest -v
open results/dashboard.html
```

The pipeline downloads the public workbook on first run. Raw data, Parquet and SQLite remain local and are excluded from Git. CSV/JSON aggregate results and the standalone dashboard are committed for inspection. The Spark execution uses `local[2]`; it demonstrates the Spark API and partitioned Parquet workflow without claiming a multi-node performance benchmark.

## Architecture

UCI workbook → validated row-preserving bronze CSV → typed PySpark facts → month-partitioned silver Parquet → monthly/country gold mart and customer cohort table → SQLite SQL, standalone dashboard and monthly review.

The same workflow runs without cloud credentials. Review changes to monetary definitions, ingestion or cohort eligibility in a pull request. Re-run boundary tests and source-to-mart reconciliation before accepting a change. AI assisted authoring does not substitute for these checks.

## Executed results

The full run processed 541,909 source lines into 314 month/country groups and 91 cohort cells across 13 observed months. All 13 reconciliation checks and seven boundary tests passed. Source hash, exact totals and local Spark execution settings are recorded in `results/validation.json`; the dashboard's month/country filters and partial-month warning were also checked in Chrome.

## Data and scope

Primary: [UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail), Daqing Chen (2015), DOI 10.24432/C5BW33, CC BY 4.0. It contains historical UK retailer invoice lines. The downloaded artifact hash and observed counts are recorded in `results/validation.json`.

Backup: [UCI Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii), Daqing Chen (2012), DOI 10.24432/C5CG6D, CC BY 4.0. It is located, not executed here. It overlaps the primary data's retailer/time window, so it is not independent validation. A separate ingestion adapter must map its `Invoice`, `Price` and `Customer ID` columns and prevent overlap duplication.

These are retail invoice records, not payment processor authorization events, subscription contracts or product telemetry. No causal effect, realized business savings, production deployment or user adoption is claimed.

## Metric definitions

Money is GBP. Each line is rounded half-up to integer pence before aggregation. Net recorded sales equal gross positive line value less absolute negative line value. Negative-price records are quarantined from monetary totals; zero-price records stay. Invoice identifiers beginning with C mark cancellations. Orders require positive quantity and a non-cancellation invoice. Missing customer IDs remain in financial totals and are excluded from cohorts. Exact repeated source lines are preserved because no unique business line key is supplied.

Retention is distinct active known buyers in a month divided by the original first-purchase cohort size. It is an observed repeat-purchase rate, not subscription NRR. There is no claim about future months beyond the dataset. December 2011 is incomplete and is excluded from complete-month comparisons. Financial totals include ledger adjustments and non-merchandise codes present in the source and are not GAAP revenue.

See `docs/schema.md`, `docs/training.md`, `self_service.sql` and the executed artifacts in `results/`.
