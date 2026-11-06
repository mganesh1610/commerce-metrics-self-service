import unittest
from pipeline import cohort_counts, line_minor, ratio


class MetricBoundaries(unittest.TestCase):
    def test_return_reduces_sales(self):
        self.assertEqual(line_minor(-3, "2.50"), -750)

    def test_negative_price_quarantined(self):
        self.assertIsNone(line_minor(1, "-1.00"))

    def test_decimal_half_up(self):
        self.assertEqual(line_minor(1, "0.005"), 1)
        self.assertEqual(line_minor(-1, "0.005"), -1)

    def test_zero_price_retained(self):
        self.assertEqual(line_minor(8, "0"), 0)

    def test_zero_denominator_unavailable(self):
        self.assertIsNone(ratio(3, 0))

    def test_missing_customer_not_in_cohorts(self):
        self.assertEqual(cohort_counts([(None, "2011-01", 1)]), {})

    def test_returns_and_repeat_lines_do_not_inflate_retention(self):
        rows = [("A", "2011-01", -1), ("A", "2011-02", 1), ("A", "2011-02", 2), ("A", "2011-03", 1)]
        self.assertEqual(cohort_counts(rows), {("2011-02", "2011-02"): 1, ("2011-02", "2011-03"): 1})


if __name__ == "__main__":
    unittest.main()
