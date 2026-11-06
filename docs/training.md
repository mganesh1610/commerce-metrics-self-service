# Stakeholder reporting walkthrough

1. Open `results/dashboard.html`. Select November 2011 and all countries. Read net recorded sales and the return-value rate. Explain why returns belong in the numerator of one measure and reduce the other.
2. Filter to one country. Compare its orders and source lines. Explain the difference between invoice and line grain. Run the first query in `self_service.sql` against `results/metrics.sqlite` to reproduce the same cut.
3. Select December 2011. Identify the partial-period warning. Use the complete-month SQL to compare November with October instead. Avoid a growth recommendation based on the incomplete month.
4. Inspect `results/cohort_retention.csv`. Identify a first-purchase cohort, its original denominator and observed subsequent buyers. Missing IDs are excluded from this analysis; they are still included in financial totals.
5. Read `results/monthly_review.md`, then propose one investigation with its metric definition, data caveat and next action. The report identifies descriptive changes; no experiment establishes causality.

Completion is demonstrated by independently reproducing a filtered value and explaining its denominator. No actual stakeholder training attendance or adoption rate is claimed.
