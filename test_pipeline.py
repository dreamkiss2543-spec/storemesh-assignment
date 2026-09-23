import unittest

from pipeline import convert_amount_to_usd, deduplicate_customers, standardize_phone


class TestTransformations(unittest.TestCase):
    def test_standardize_phone(self):
        self.assertEqual(standardize_phone("+1 (555) 123-4567"), "15551234567")
        self.assertEqual(standardize_phone("081-234-5678"), "0812345678")
        self.assertIsNone(standardize_phone(None))

    def test_deduplicate_customers_unsorted(self):
        old = (1, "Old Name", "old@example.com", None, "2023-01-01")
        latest = (1, "New Name", "new@example.com", None, "2023-06-01")
        other = (2, "Other", "other@example.com", None, "2023-02-01")

        result = deduplicate_customers.fn([old, other, latest])

        self.assertEqual(result, {1: latest, 2: other})
    def test_convert_amount_to_usd(self):
        rates = {
            ("THB", "2023-01-01"): 0.03,
            (None, "2023-01-01"): 0.5,
        }

        self.assertEqual(
            convert_amount_to_usd(100, "THB", "2023-01-01", rates),
            (3.0, False),
        )
        self.assertEqual(
            convert_amount_to_usd(100, "THB", "2023-01-02", rates),
            (100, True),
        )
        self.assertEqual(
            convert_amount_to_usd(20.25, "USD", "2023-01-01", rates),
            (20.25, False),
        )
        self.assertEqual(
            convert_amount_to_usd(50, None, "2023-01-01", rates),
            (50, True),
        )

if __name__ == "__main__":
    unittest.main()