"""Regression tests for YES/NO token resolution and YES-implied midpoint."""

import unittest

from polymarket_tools import api
from polymarket_tools.scanner import _get_token_ids


class TestGetTokenIds(unittest.TestCase):
    def test_ordered_yes_no(self):
        m = {
            "tokens": [
                {"token_id": "a", "outcome": "Yes"},
                {"token_id": "b", "outcome": "No"},
            ]
        }
        self.assertEqual(_get_token_ids(m), ("a", "b"))

    def test_reversed_no_yes(self):
        m = {
            "tokens": [
                {"token_id": "b", "outcome": "No"},
                {"token_id": "a", "outcome": "Yes"},
            ]
        }
        self.assertEqual(_get_token_ids(m), ("a", "b"))

    def test_scan_finds_yes_no_not_only_first_two(self):
        m = {
            "tokens": [
                {"token_id": "x", "outcome": "Other"},
                {"token_id": "a", "outcome": "Yes"},
                {"token_id": "b", "outcome": "No"},
            ]
        }
        self.assertEqual(_get_token_ids(m), ("a", "b"))

    def test_short_y_n_labels(self):
        m = {
            "tokens": [
                {"token_id": "b", "outcome": "N"},
                {"token_id": "a", "outcome": "Y"},
            ]
        }
        self.assertEqual(_get_token_ids(m), ("a", "b"))


class TestYesMidFromNoBook(unittest.TestCase):
    """Mirror _persist_market: NO book mid implies YES mid = 1 - mid_no."""

    def test_no_book_fallback_flip(self):
        best_bid_yes, best_ask_yes = None, None
        best_bid_no, best_ask_no = 0.25, 0.35
        mid_yes, spread_yes = api.compute_midpoint_and_spread(best_bid_yes, best_ask_yes)
        self.assertIsNone(mid_yes)
        mid_no, spread_no = api.compute_midpoint_and_spread(best_bid_no, best_ask_no)
        self.assertIsNotNone(mid_no)
        mid_yes = 1.0 - mid_no
        spread_yes = spread_no
        self.assertAlmostEqual(mid_no, 0.30)
        self.assertAlmostEqual(mid_yes, 0.70)
        self.assertAlmostEqual(spread_yes, spread_no)


if __name__ == "__main__":
    unittest.main()
