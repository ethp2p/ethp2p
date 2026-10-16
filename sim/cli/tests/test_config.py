from __future__ import annotations

import unittest

from pydantic import ValidationError

from simctl.config import StrategyConfig, get_strategy_dir_name


class StrategyConfigTests(unittest.TestCase):
    def test_rs_chunk_length_is_a_parameter_and_bitmaps_default_enabled(self) -> None:
        strategy = StrategyConfig(name="RS", chunk_len=16_384)

        self.assertEqual(
            get_strategy_dir_name(strategy, num_nodes=10, msg_size=100_000),
            "RS-d16-p16-cl16384-bm1-t100-n10-100000",
        )

    def test_disable_bitmap_controls_directory_bitmap_suffix(self) -> None:
        strategy = StrategyConfig(name="RS", disable_bitmap=True)

        self.assertEqual(
            get_strategy_dir_name(strategy, num_nodes=10, msg_size=100_000),
            "RS-d16-p16-bm0-t100-n10-100000",
        )

    def test_unsupported_strategy_name_is_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            StrategyConfig(name="unsupported")  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
