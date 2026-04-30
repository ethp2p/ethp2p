from __future__ import annotations

import unittest

from pydantic import ValidationError

from simctl.config import StrategyConfig, get_strategy_dir_name, strategy_for_go


class ConfigTests(unittest.TestCase):
    def test_rlnc_strategy_config_is_accepted(self) -> None:
        strat = StrategyConfig(name="RLNC", num_chunks=32)

        self.assertEqual(strat.name, "RLNC")
        self.assertEqual(strat.num_chunks, 32)

    def test_rlnc_strategy_dir_name_includes_rlnc_parameters(self) -> None:
        strat = StrategyConfig(
            name="RLNC",
            num_chunks=32,
            target_chunk_size=16384,
            num_chunks_per_generation=16,
            origin_redundancy=8,
        )

        self.assertEqual(
            get_strategy_dir_name(strat, num_nodes=1000, msg_size=2_000_000),
            "RLNC-nc32-tcs16384-ncpg16-or8-n1000-2000000",
        )

    def test_rlnc_rejects_rs_chunk_len_field(self) -> None:
        with self.assertRaisesRegex(ValidationError, "target_chunk_size"):
            StrategyConfig(name="RLNC", chunk_len=16384)

    def test_rlnc_target_chunk_size_requires_generation_size(self) -> None:
        with self.assertRaisesRegex(ValidationError, "num_chunks_per_generation"):
            StrategyConfig(name="RLNC", target_chunk_size=16384)

    def test_resolved_rlnc_strategy_omits_rs_fields(self) -> None:
        strat = StrategyConfig(
            name="RLNC",
            target_chunk_size=16384,
            num_chunks_per_generation=16,
        )

        self.assertEqual(
            strategy_for_go(strat),
            {
                "name": "RLNC",
                "num_chunks": 16,
                "num_chunks_per_generation": 16,
                "target_chunk_size": 16384,
                "origin_redundancy": 0,
                "forward_multiplier": 4,
            },
        )


if __name__ == "__main__":
    unittest.main()
