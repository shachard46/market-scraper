"""Regression tests for YES/NO correction classification."""

import unittest

from polymarket_tools.migrations.correct_yes_no_prices import (
    GammaBinaryMarket,
    _ltp_suspicious_not_proof,
    classify_token_alignment,
    repair_price_swap_when_gamma_clear,
)


def _gamma(yes_price=0.70, no_price=0.30) -> GammaBinaryMarket:
    return GammaBinaryMarket(
        market_id="m",
        yes_token_id="yes_id",
        no_token_id="no_id",
        yes_price=yes_price,
        no_price=no_price,
        raw={},
    )


class TestTokenAlignment(unittest.TestCase):
    def test_aligned_tokens(self):
        self.assertEqual(classify_token_alignment("yes_id", "no_id", _gamma()), "aligned")

    def test_reversed_tokens(self):
        self.assertEqual(classify_token_alignment("no_id", "yes_id", _gamma()), "reversed")

    def test_mismatched_tokens(self):
        self.assertEqual(classify_token_alignment("other_yes", "other_no", _gamma()), "mismatch")


class TestGammaClearRepair(unittest.TestCase):
    def test_swaps_only_clear_gamma_inversion(self):
        self.assertTrue(repair_price_swap_when_gamma_clear(0.31, 0.69, _gamma()))

    def test_noops_when_aligned(self):
        self.assertFalse(repair_price_swap_when_gamma_clear(0.69, 0.31, _gamma()))

    def test_noops_when_gamma_is_ambiguous(self):
        self.assertFalse(
            repair_price_swap_when_gamma_clear(0.31, 0.69, _gamma(yes_price=0.51, no_price=0.49))
        )

    def test_ltp_suspicious_is_report_only(self):
        self.assertTrue(
            _ltp_suspicious_not_proof(0.70, 0.30, 0.31, epsilon=0.04)
        )
        self.assertFalse(repair_price_swap_when_gamma_clear(0.70, 0.30, _gamma()))


if __name__ == "__main__":
    unittest.main()
