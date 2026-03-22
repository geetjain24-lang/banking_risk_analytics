import sys
import os
import unittest

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.screening import screen_customer


class TestScreening(unittest.TestCase):
    def setUp(self):
        self.world_check_df = pd.DataFrame([
            {
                "entity_id": "WC001",
                "entity_name": "Mohammed bin Salman Al Saud",
                "dob": "1985-08-31",
                "nationality": "SA",
                "category": "PEP",
                "listed_date": "2017-06-21",
            },
            {
                "entity_id": "WC002",
                "entity_name": "Elena Petrova",
                "dob": "1982-05-18",
                "nationality": "RU",
                "category": "Sanctions",
                "listed_date": "2022-03-15",
            },
        ])

    def test_exact_name_match(self):
        customer = pd.Series({
            "full_name": "Elena Petrova",
            "dob": "1982-05-18",
            "nationality": "RU",
        })
        result = screen_customer(customer, self.world_check_df, threshold=85)
        self.assertIsNotNone(result)
        self.assertEqual(result["matched_entity_id"], "WC002")
        self.assertEqual(result["match_score"], 100)

    def test_fuzzy_name_match(self):
        customer = pd.Series({
            "full_name": "Mohammed Bin Salman",
            "dob": "1985-08-31",
            "nationality": "SA",
        })
        result = screen_customer(customer, self.world_check_df, threshold=85)
        self.assertIsNotNone(result)
        self.assertEqual(result["matched_entity_id"], "WC001")
        self.assertGreaterEqual(result["match_score"], 85)

    def test_no_match(self):
        customer = pd.Series({
            "full_name": "David Lee Chang",
            "dob": "1998-02-20",
            "nationality": "US",
        })
        result = screen_customer(customer, self.world_check_df, threshold=85)
        self.assertIsNone(result)

    def test_low_threshold_returns_match(self):
        customer = pd.Series({
            "full_name": "Elena P",
            "dob": "1982-05-18",
            "nationality": "RU",
        })
        # With a very low threshold, a partial match should still hit
        result = screen_customer(customer, self.world_check_df, threshold=50)
        self.assertIsNotNone(result)

    def test_high_threshold_rejects_partial(self):
        customer = pd.Series({
            "full_name": "Elena P",
            "dob": "1982-05-18",
            "nationality": "RU",
        })
        result = screen_customer(customer, self.world_check_df, threshold=95)
        self.assertIsNone(result)


class TestReconciliation(unittest.TestCase):
    def test_reconcile_classification(self):
        from src.reconciliation import reconcile

        config = {"screening": {"fuzzy_match_threshold": 85}}
        screened_df = pd.DataFrame([
            {
                "customer_id": "C001",
                "source_system": "housing_loan",
                "full_name": "Test User",
                "dob": "1990-01-01",
                "nationality": "US",
                "matched_entity_id": "WC001",
                "matched_entity_name": "Test Entity",
                "match_score": 92,
                "category": "PEP",
                "dob_match": True,
                "nationality_match": True,
            },
            {
                "customer_id": "C002",
                "source_system": "credit_card",
                "full_name": "Clean User",
                "dob": "1995-06-15",
                "nationality": "GB",
                "matched_entity_id": None,
                "matched_entity_name": None,
                "match_score": 0,
                "category": None,
                "dob_match": False,
                "nationality_match": False,
            },
        ])

        result = reconcile(config, screened_df)
        self.assertEqual(result.iloc[0]["screening_status"], "HIT")
        self.assertEqual(result.iloc[1]["screening_status"], "NO HIT")
        self.assertIn("recon_timestamp", result.columns)


if __name__ == "__main__":
    unittest.main()
