"""Merge per-node Shadow trace files into a single .bctrace."""

import json
from pathlib import Path

from simctl.config import Config, StrategyConfig
from simctl.topology import Topology


def merge_shadow_traces(
    shadow_data: Path,
    topology: Topology,
    strat: StrategyConfig,
    output: Path,
) -> int:
    """Read per-node events.ndjson from Shadow output and write unified .bctrace.

    Each Shadow node writes a full trace file (header + events + footer)
    to shadow.data/hosts/nodeN/events.ndjson. We extract only the event
    lines from each, merge by timestamp, and write a single .bctrace
    with a unified header and footer.

    Returns the total number of events merged.
    """
    hosts_dir = shadow_data / "hosts"
    if not hosts_dir.is_dir():
        raise FileNotFoundError(f"Shadow hosts directory not found: {hosts_dir}")

    all_events: list[list] = []
    # Each node writes the same header, so the first one seen carries the
    # run-wide metadata. Lifting it here avoids re-deriving peer IDs in Python,
    # which would duplicate the Go identity derivation and could drift from it.
    node_header: dict | None = None

    for node in topology.nodes:
        events_path = hosts_dir / f"node{node.num}" / "events.ndjson"
        if not events_path.exists():
            continue

        with open(events_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    parsed = json.loads(line)
                except json.JSONDecodeError:
                    continue
                # Event lines are arrays: [timestamp_us, node_idx, code, ...]
                # Header is the dict with a "v" key; footer has "end".
                if isinstance(parsed, dict) and "v" in parsed:
                    if node_header is None:
                        node_header = parsed
                elif isinstance(parsed, list) and len(parsed) >= 3:
                    all_events.append(parsed)

    all_events.sort(key=lambda ev: ev[0])

    nodes = [f"n{n.num}" for n in topology.nodes]
    topo_dict = topology.to_dict()
    strat_dict = strat.model_dump(exclude_none=True)

    header = {
        "v": 1,
        "t0": "1970-01-01T00:00:00Z",
        "nodes": nodes,
        "topology": topo_dict,
        "cfg": strat_dict,
    }

    # Preserve the decoder and peer-ID mapping so consumers can resolve event
    # peer IDs back to nodes. ethp2p peer IDs are transport identities rather
    # than node numbers, so without this the events are unresolvable.
    for key in ("decoderName", "peer_ids"):
        if node_header and node_header.get(key):
            header[key] = node_header[key]

    with open(output, "w") as f:
        f.write(json.dumps(header) + "\n")
        for ev in all_events:
            f.write(json.dumps(ev) + "\n")
        footer = {"end": True, "duration": all_events[-1][0] if all_events else 0, "events": len(all_events)}
        f.write(json.dumps(footer) + "\n")

    return len(all_events)
