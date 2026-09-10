from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from simctl.config import StrategyConfig
from simctl.topology import Edge, NodeSpec, Topology
from simctl.trace_merge import merge_shadow_traces


def make_topology(nums: list[int]) -> Topology:
    return Topology(
        nodes=[NodeSpec(num=n, upload_bw_mbps=50, download_bw_mbps=50, country="US") for n in nums],
        edges=[Edge(source=0, target=1, latency_ms=50)],
    )


class TraceMergeTests(unittest.TestCase):
    def write_host(self, hosts: Path, num: int, header: dict, events: list[list]) -> None:
        node_dir = hosts / f"node{num}"
        node_dir.mkdir(parents=True)
        with open(node_dir / "events.ndjson", "w") as f:
            f.write(json.dumps(header) + "\n")
            for ev in events:
                f.write(json.dumps(ev) + "\n")
            f.write(json.dumps({"end": True, "duration": 0, "events": len(events)}) + "\n")

    def test_merges_events_from_all_nodes_in_timestamp_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            hosts = tmp_path / "shadow.data" / "hosts"
            self.write_host(hosts, 0, {"v": 1}, [[20, 0, "ph"], [10, 0, "ps"]])
            self.write_host(hosts, 1, {"v": 1}, [[15, 1, "ph"]])

            out = tmp_path / "merged.bctrace"
            total = merge_shadow_traces(
                tmp_path / "shadow.data", make_topology([0, 1]),
                StrategyConfig(name="RS"), out,
            )

            self.assertEqual(total, 3)
            lines = out.read_text().strip().split("\n")
            header = json.loads(lines[0])
            events = [json.loads(line) for line in lines[1:-1]]
            self.assertEqual(header["nodes"], ["n0", "n1"])
            self.assertEqual([ev[0] for ev in events], [10, 15, 20])

    def test_preserves_peer_id_mapping_and_decoder(self) -> None:
        """ethp2p events carry transport peer IDs, not node numbers, so the
        mapping must survive the merge or events are unresolvable."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            hosts = tmp_path / "shadow.data" / "hosts"
            header = {
                "v": 1,
                "decoderName": "ethp2p",
                "peer_ids": ["peer-a", "peer-b"],
            }
            self.write_host(hosts, 0, header, [[10, 0, "ph", "peer-b"]])
            self.write_host(hosts, 1, header, [[20, 1, "ph", "peer-a"]])

            out = tmp_path / "merged.bctrace"
            merge_shadow_traces(
                tmp_path / "shadow.data", make_topology([0, 1]),
                StrategyConfig(name="RS"), out,
            )

            merged = json.loads(out.read_text().split("\n")[0])
            self.assertEqual(merged["decoderName"], "ethp2p")
            self.assertEqual(merged["peer_ids"], ["peer-a", "peer-b"])

    def test_omits_peer_ids_when_nodes_did_not_record_them(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            hosts = tmp_path / "shadow.data" / "hosts"
            self.write_host(hosts, 0, {"v": 1}, [[10, 0, "ph"]])

            out = tmp_path / "merged.bctrace"
            merge_shadow_traces(
                tmp_path / "shadow.data", make_topology([0, 1]),
                StrategyConfig(name="RS"), out,
            )

            merged = json.loads(out.read_text().split("\n")[0])
            self.assertNotIn("peer_ids", merged)
            self.assertNotIn("decoderName", merged)


if __name__ == "__main__":
    unittest.main()
