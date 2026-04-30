from __future__ import annotations

import unittest
from contextlib import redirect_stdout
from io import StringIO

from simctl.analyze import MessageStats, RunStats, print_table


class AnalyzeReportTests(unittest.TestCase):
    def test_gossipsub_chunk_cells_render_as_dash(self) -> None:
        rs = RunStats(
            name="RS",
            num_nodes=2,
            message_size=1024,
            expected=1,
            messages={
                0: MessageStats(
                    delivered=1,
                    origin_sent=1024,
                    relay_verdicts={"accepted": [3]},
                )
            },
        )
        gossipsub = RunStats(
            name="gossipsub",
            num_nodes=2,
            message_size=1024,
            expected=1,
            messages={
                0: MessageStats(
                    delivered=1,
                    origin_sent=1024,
                    relay_verdicts={"accepted": [0]},
                )
            },
        )

        out = StringIO()
        with redirect_stdout(out):
            print_table([rs, gossipsub])

        accepted_p50 = next(
            line for line in out.getvalue().splitlines() if "chunks accepted p50" in line
        )
        self.assertIn("3", accepted_p50)
        self.assertTrue(accepted_p50.rstrip().endswith("-"), accepted_p50)


if __name__ == "__main__":
    unittest.main()
