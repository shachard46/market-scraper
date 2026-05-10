"""Regression tests for YES/NO token resolution and YES-implied midpoint."""

import unittest

from polymarket_tools import api
from polymarket_tools.scanner import _binary_market_view, _get_token_ids, choose_yes_price


def _book(bid=None, ask=None, last=None):
    book = {
        "bids": [] if bid is None else [{"price": str(bid), "size": "1"}],
        "asks": [] if ask is None else [{"price": str(ask), "size": "1"}],
    }
    if last is not None:
        book["last_trade_price"] = str(last)
    return book


class TestBuildTokensYesFirst(unittest.TestCase):
    """_build_tokens must always place the YES token at index 0."""

    def test_yes_no_order_preserved(self):
        tokens = api._build_tokens(["a", "b"], ["Yes", "No"], ["0.7", "0.3"])
        self.assertEqual(tokens[0]["token_id"], "a")
        self.assertEqual(tokens[0]["outcome"], "Yes")
        self.assertEqual(tokens[1]["token_id"], "b")
        self.assertEqual(tokens[1]["outcome"], "No")

    def test_no_yes_order_corrected(self):
        """Gamma returns No first — _build_tokens must sort YES to index 0."""
        tokens = api._build_tokens(["b", "a"], ["No", "Yes"], ["0.3", "0.7"])
        self.assertEqual(tokens[0]["token_id"], "a")
        self.assertEqual(tokens[0]["outcome"], "Yes")
        self.assertAlmostEqual(tokens[0]["price"], 0.7)
        self.assertEqual(tokens[1]["token_id"], "b")
        self.assertEqual(tokens[1]["outcome"], "No")
        self.assertAlmostEqual(tokens[1]["price"], 0.3)

    def test_short_y_n_labels_sorted(self):
        tokens = api._build_tokens(["b", "a"], ["N", "Y"], ["0.4", "0.6"])
        self.assertEqual(tokens[0]["outcome"], "Y")
        self.assertEqual(tokens[1]["outcome"], "N")

    def test_prices_follow_token_after_sort(self):
        """Price must stay attached to its token after the sort."""
        tokens = api._build_tokens(["no_id", "yes_id"], ["No", "Yes"], ["0.28", "0.72"])
        self.assertEqual(tokens[0]["token_id"], "yes_id")
        self.assertAlmostEqual(tokens[0]["price"], 0.72)
        self.assertEqual(tokens[1]["token_id"], "no_id")
        self.assertAlmostEqual(tokens[1]["price"], 0.28)

    def test_winner_flag_follows_token_after_sort(self):
        """Winner flag must stay with the resolved token."""
        tokens = api._build_tokens(["no_id", "yes_id"], ["No", "Yes"], ["0.0", "1.0"])
        self.assertFalse(tokens[1]["winner"])
        self.assertTrue(tokens[0]["winner"])


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

    def test_unknown_labels_do_not_fall_back_to_position(self):
        m = {
            "tokens": [
                {"token_id": "a", "outcome": "Up"},
                {"token_id": "b", "outcome": "Down"},
            ]
        }
        self.assertEqual(_get_token_ids(m), (None, None))
        self.assertIsNone(_binary_market_view(m))


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


class TestChooseYesPrice(unittest.TestCase):
    def test_tight_yes_midpoint_beats_ltp(self):
        yes_price, no_price, midpoint, spread = choose_yes_price(
            _book(0.62, 0.66, last=0.20),
            _book(0.34, 0.38),
            gamma_yes_price=0.30,
            yes_last_trade_price=0.20,
        )
        self.assertAlmostEqual(yes_price, 0.64)
        self.assertAlmostEqual(no_price, 0.36)
        self.assertAlmostEqual(midpoint, 0.64)
        self.assertAlmostEqual(spread, 0.04)

    def test_wide_meaningless_midpoint_uses_yes_ltp(self):
        yes_price, no_price, midpoint, spread = choose_yes_price(
            _book(0.20, 0.80, last=0.72),
            _book(0.20, 0.80),
            gamma_yes_price=0.68,
            yes_last_trade_price=0.72,
        )
        self.assertAlmostEqual(yes_price, 0.72)
        self.assertAlmostEqual(no_price, 0.28)
        self.assertAlmostEqual(midpoint, 0.50)
        self.assertAlmostEqual(spread, 0.60)

    def test_no_book_tight_midpoint_is_complemented(self):
        yes_price, no_price, midpoint, spread = choose_yes_price(
            None,
            _book(0.25, 0.31),
            gamma_yes_price=None,
            yes_last_trade_price=None,
        )
        self.assertAlmostEqual(yes_price, 0.72)
        self.assertAlmostEqual(no_price, 0.28)
        self.assertAlmostEqual(midpoint, 0.72)
        self.assertAlmostEqual(spread, 0.06)

    def test_neutral_midpoint_falls_back_to_gamma_when_no_ltp(self):
        yes_price, no_price, midpoint, spread = choose_yes_price(
            _book(0.49, 0.51),
            None,
            gamma_yes_price=0.67,
            yes_last_trade_price=None,
        )
        self.assertAlmostEqual(yes_price, 0.67)
        self.assertAlmostEqual(no_price, 0.33)
        self.assertAlmostEqual(midpoint, 0.50)
        self.assertAlmostEqual(spread, 0.02)


if __name__ == "__main__":
    unittest.main()
