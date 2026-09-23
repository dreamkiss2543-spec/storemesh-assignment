import unittest

from pipeline import convert_amount_to_usd, standardize_phone


class TestTransformations(unittest.TestCase):
    def test_standardize_phone(self):
        self.assertEqual(standardize_phone("+1 (555) 123-4567"), "15551234567")
        self.assertEqual(standardize_phone("081-234-5678"), "0812345678")
        self.assertIsNone(standardize_phone(None))

    def test_convert_amount_to_usd(self):
        rates = {("THB", "2023-01-01"): 0.03}

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