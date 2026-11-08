# Schema and validation

Grain: one original workbook line. `row_id` is a reproducible ingestion key.

| Source | Bronze / silver | Type / treatment |
|---|---|---|
| InvoiceNo | invoice_no | string, preserve C prefix |
| StockCode | stock_code | string, preserve non-numeric adjustment codes |
| Description | description | string, blank permitted |
| Quantity | quantity | signed integer |
| InvoiceDate | invoice_at | Europe/London local timestamp, source has no explicit offset |
| UnitPrice | unit_price | Decimal(18,6), GBP |
| CustomerID | customer_id | nullable string identifier |
| Country | country | string |

Derived fields: `month` (YYYY-MM), `is_cancellation`, `valid_price`, `amount_minor` (signed integer pence). Gold grain: `(month,country)`; customer cohort grain: `(cohort_month,month)`.

Monthly outputs: source lines, financially valid lines, net/gross/return pence, distinct positive non-cancellation invoices, known buyers, return-value rate and partial-month flag. Buyer counts are not additive across country cells. `net_per_order_gbp` includes all valid ledger adjustments in the numerator and is not conventional positive-order AOV.

Checks compare independent Python Decimal controls to Spark row/financial aggregates, reconcile mart totals, verify ingestion key uniqueness, preserve missing-ID records in financial accounting, bound retention rates and check SQLite integrity. Tests exercise zero denominators, signed returns, half-up rounding, invalid prices and cohort eligibility.

The backup workbook requires a separate `Invoice`→`invoice_no`, `StockCode`→`stock_code`, `Description`→`description`, `Quantity`→`quantity`, `InvoiceDate`→`invoice_at`, `Price`→`unit_price`, `Customer ID`→`customer_id`, `Country`→`country` adapter. Never append it blindly to the primary data because the datasets overlap.
