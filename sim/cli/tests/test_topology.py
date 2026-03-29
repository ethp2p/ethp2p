from __future__ import annotations

import random
import unittest

from simctl.topology import (
    generate_random_topology,
    generate_realistic_topology,
    generate_ring_topology,
    get_bandwidth,
)


class TopologyTests(unittest.TestCase):
    def test_origin_bandwidth_never_uses_supernode_profile(self) -> None:
        up, down = get_bandwidth(0, super_node_fraction=1.0, rng=random.Random(123))
        self.assertEqual((up, down), (50, 100))

    def test_non_origin_can_still_be_supernode(self) -> None:
        up, down = get_bandwidth(1, super_node_fraction=1.0, rng=random.Random(123))
        self.assertEqual((up, down), (1024, 1024))

    def test_generated_topologies_never_make_origin_a_supernode(self) -> None:
        topologies = [
            generate_random_topology(12, degree=4, seed=1, super_node_fraction=1.0),
            generate_ring_topology(12, seed=1, super_node_fraction=1.0),
            generate_realistic_topology(12, degree=4, seed=1, super_node_fraction=1.0),
        ]

        for topo in topologies:
            origin = topo.nodes[0]
            self.assertEqual((origin.upload_bw_mbps, origin.download_bw_mbps), (50, 100))


if __name__ == "__main__":
    unittest.main()
