from __future__ import annotations

import unittest

from simctl.config import StrategyConfig, get_strategy_dir_name


class ConfigTests(unittest.TestCase):
    def test_rlnc_strategy_config_is_accepted(self) -> None:
        strat = StrategyConfig(name="RLNC", num_chunks=32)

        self.assertEqual(strat.name, "RLNC")
        self.assertEqual(strat.num_chunks, 32)

    def test_rlnc_strategy_dir_name_includes_rlnc_parameters(self) -> None:
        strat = StrategyConfig(name="RLNC-ChunkLen", num_chunks=32, chunk_len=16384)

        self.assertEqual(
            get_strategy_dir_name(strat, num_nodes=1000, msg_size=2_000_000),
            "RLNC-ChunkLen-nc32-cl16384-bm1-t100-n1000-2000000",
        )


if __name__ == "__main__":
    unittest.main()
