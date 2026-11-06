-- Open results/metrics.sqlite in SQLite. All money is in original GBP minor units.
-- Last complete month: retain financial definitions across all cuts.
SELECT country, net_minor / 100.0 AS net_sales_gbp, orders,
       returns_minor * 1.0 / NULLIF(gross_minor, 0) AS return_value_rate
FROM monthly_metrics WHERE month = '2011-11'
ORDER BY net_minor DESC;

-- Month-over-month comparison excludes the incomplete December 2011 period.
WITH complete AS (
  SELECT month, SUM(net_minor) AS net_minor FROM monthly_metrics
  WHERE partial_month = 0 GROUP BY month
), lagged AS (
  SELECT *, LAG(net_minor) OVER (ORDER BY month) AS previous_net FROM complete
)
SELECT month, net_minor / 100.0 AS net_sales_gbp,
       (net_minor - previous_net) * 1.0 / NULLIF(previous_net, 0) AS mom_rate
FROM lagged ORDER BY month;
