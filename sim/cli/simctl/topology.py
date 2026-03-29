"""Topology generation with latency computation."""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from simctl.config import TopologyConfig


@dataclass
class NodeSpec:
    num: int
    upload_bw_mbps: int
    download_bw_mbps: int
    country: str


@dataclass
class Edge:
    source: int
    target: int
    latency_ms: int


@dataclass
class Topology:
    nodes: list[NodeSpec]
    edges: list[Edge]

    def to_dict(self) -> dict:
        return {
            "nodes": [
                {
                    "num": n.num,
                    "upload_bw_mbps": n.upload_bw_mbps,
                    "download_bw_mbps": n.download_bw_mbps,
                    "country": n.country,
                }
                for n in self.nodes
            ],
            "edges": [
                {"source": e.source, "target": e.target, "latency_ms": e.latency_ms}
                for e in self.edges
            ],
        }

    def save(self, path: Path) -> None:
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)


def load_topology(path: Path) -> Topology:
    """Load a topology from a JSON file."""
    with open(path) as f:
        data = json.load(f)
    nodes = [
        NodeSpec(
            num=n["num"],
            upload_bw_mbps=n["upload_bw_mbps"],
            download_bw_mbps=n["download_bw_mbps"],
            country=n["country"],
        )
        for n in data["nodes"]
    ]
    edges = [
        Edge(source=e["source"], target=e["target"], latency_ms=e["latency_ms"])
        for e in data["edges"]
    ]
    # Ensure bidirectionality: if (u,v) exists but (v,u) doesn't, add (v,u).
    existing = {(e.source, e.target): e.latency_ms for e in edges}
    for (u, v), latency in list(existing.items()):
        if (v, u) not in existing:
            edges.append(Edge(source=v, target=u, latency_ms=latency))
            existing[(v, u)] = latency
    return Topology(nodes=nodes, edges=edges)


def load_latencies() -> dict[str, dict[str, int]]:
    """Load country-to-country latencies."""
    data_dir = Path(__file__).parent.parent / "data"
    with open(data_dir / "country_latencies.json") as f:
        return json.load(f)


def load_weights() -> dict[str, int]:
    """Load country weights for random selection."""
    data_dir = Path(__file__).parent.parent / "data"
    with open(data_dir / "country_weights.json") as f:
        return json.load(f)


class CountrySelector:
    def __init__(self, weights: dict[str, int], rng: random.Random):
        self.countries = list(weights.keys())
        self.cumulative = []
        total = 0
        for c in self.countries:
            total += weights[c]
            self.cumulative.append(total)
        self.total = total
        self.rng = rng

    def select(self) -> str:
        r = self.rng.randint(0, self.total - 1)
        for i, cum in enumerate(self.cumulative):
            if r < cum:
                return self.countries[i]
        return self.countries[-1]


def get_bandwidth(
    node_num: int, super_node_fraction: float, rng: random.Random
) -> tuple[int, int]:
    """Determine upload/download bandwidth for a node."""
    if node_num == 0:
        return 50, 100  # Block builder

    if rng.random() < super_node_fraction:
        return 1024, 1024
    return 50, 50  # Regular node


def generate_random_topology(
    num_nodes: int,
    degree: int,
    seed: int,
    super_node_fraction: float = 0.0,
    origin_degree: int = 0,
) -> Topology:
    """Generate a random topology with the given parameters."""
    rng = random.Random(seed)
    weights = load_weights()
    latencies = load_latencies()
    selector = CountrySelector(weights, rng)

    # Create nodes
    nodes = []
    for i in range(num_nodes):
        up, down = get_bandwidth(i, super_node_fraction, rng)
        nodes.append(
            NodeSpec(
                num=i,
                upload_bw_mbps=up,
                download_bw_mbps=down,
                country=selector.select(),
            )
        )

    def node_degree(u: int) -> int:
        if u == 0 and origin_degree > 0:
            return origin_degree
        return degree

    # Create edges - first ensure connectivity
    adjacency: dict[int, set[int]] = {i: set() for i in range(num_nodes)}
    for i in range(1, num_nodes):
        j = rng.randint(0, i - 1)
        adjacency[i].add(j)
        adjacency[j].add(i)

    # Add more edges to reach desired degree per node
    max_attempts = num_nodes * 10
    for u in range(num_nodes):
        target = node_degree(u)
        attempts = 0
        while len(adjacency[u]) < target and attempts < max_attempts:
            v = rng.randint(0, num_nodes - 1)
            if v != u and v not in adjacency[u] and len(adjacency[v]) < node_degree(v):
                adjacency[u].add(v)
                adjacency[v].add(u)
            attempts += 1

    # Convert to edges with latencies
    edges = []
    for u, neighbors in adjacency.items():
        for v in neighbors:
            src_country = nodes[u].country
            dst_country = nodes[v].country
            latency = latencies.get(src_country, {}).get(dst_country, 100)
            edges.append(Edge(source=u, target=v, latency_ms=latency))
    return Topology(nodes=nodes, edges=edges)


def generate_ring_topology(
    num_nodes: int,
    seed: int,
    super_node_fraction: float = 0.0,
) -> Topology:
    """Generate a ring topology."""
    rng = random.Random(seed)
    weights = load_weights()
    latencies = load_latencies()
    selector = CountrySelector(weights, rng)

    nodes = []
    for i in range(num_nodes):
        up, down = get_bandwidth(i, super_node_fraction, rng)
        nodes.append(
            NodeSpec(
                num=i,
                upload_bw_mbps=up,
                download_bw_mbps=down,
                country=selector.select(),
            )
        )

    edges = []
    for i in range(num_nodes):
        j = (i + 1) % num_nodes
        src_country = nodes[i].country
        dst_country = nodes[j].country
        latency = latencies.get(src_country, {}).get(dst_country, 100)
        edges.append(Edge(source=i, target=j, latency_ms=latency))

    return Topology(nodes=nodes, edges=edges)


def generate_realistic_topology(
    num_nodes: int,
    degree: int,
    seed: int,
    super_node_fraction: float = 0.0,
    origin_degree: int = 0,
    origin_country: str = "united states",
) -> Topology:
    """Generate a topology with latency-aware peer selection.

    The origin is placed in origin_country. Each node's peers are selected
    in three tiers based on latency: 3 near, 3 medium, 2 far (for degree=8).
    For other degrees, the ratio is preserved: ~3/8 near, 3/8 medium, 2/8 far.
    """
    rng = random.Random(seed)
    weights = load_weights()
    latencies = load_latencies()
    selector = CountrySelector(weights, rng)

    nodes = []
    for i in range(num_nodes):
        up, down = get_bandwidth(i, super_node_fraction, rng)
        country = origin_country if i == 0 else selector.select()
        nodes.append(NodeSpec(num=i, upload_bw_mbps=up, download_bw_mbps=down, country=country))

    def node_degree(u: int) -> int:
        if u == 0 and origin_degree > 0:
            return origin_degree
        return degree

    def latency_between(u: int, v: int) -> int:
        return latencies.get(nodes[u].country, {}).get(nodes[v].country, 100)

    def select_tiered_peers(
        u: int, candidates: list[int], target: int
    ) -> list[int]:
        """Pick peers in three latency tiers: near, medium, far."""
        if len(candidates) <= target:
            return candidates

        scored = [(v, latency_between(u, v)) for v in candidates]
        scored.sort(key=lambda x: x[1])

        n_near = (target * 3 + 7) // 8   # ceil(3/8 * target)
        n_mid = (target * 3 + 7) // 8
        n_far = target - n_near - n_mid

        # Split candidates into thirds by latency rank
        third = len(scored) // 3
        near_pool = scored[:third] if third > 0 else scored
        mid_pool = scored[third : 2 * third] if third > 0 else scored
        far_pool = scored[2 * third :] if third > 0 else scored

        selected_set: set[int] = set()
        selected: list[int] = []

        for pool, n in [(near_pool, n_near), (mid_pool, n_mid), (far_pool, n_far)]:
            available = [(v, lat) for v, lat in pool if v not in selected_set]
            if not available or n <= 0:
                continue
            pick = min(n, len(available))
            for v, _ in rng.sample(available, pick):
                selected.append(v)
                selected_set.add(v)

        # Fill remaining from any tier if a pool was too small
        if len(selected) < target:
            remaining = [v for v, _ in scored if v not in selected_set]
            rng.shuffle(remaining)
            selected.extend(remaining[: target - len(selected)])

        return selected[:target]

    # Build adjacency with tiered selection first, then patch connectivity.
    adjacency: dict[int, set[int]] = {i: set() for i in range(num_nodes)}

    # Phase 1: tiered peer selection for all nodes
    for u in range(num_nodes):
        target = node_degree(u)
        if len(adjacency[u]) >= target:
            continue
        need = target - len(adjacency[u])
        candidates = [
            v for v in range(num_nodes)
            if v != u and v not in adjacency[u] and len(adjacency[v]) < node_degree(v)
        ]
        peers = select_tiered_peers(u, candidates, need)
        for v in peers:
            adjacency[u].add(v)
            adjacency[v].add(u)

    # Phase 2: ensure connectivity via BFS; add bridging edges where needed.
    # Find connected components, then link them by replacing the
    # least-valuable edge in each isolated component.
    visited = [False] * num_nodes
    components: list[list[int]] = []
    for start in range(num_nodes):
        if visited[start]:
            continue
        comp: list[int] = []
        stack = [start]
        while stack:
            v = stack.pop()
            if visited[v]:
                continue
            visited[v] = True
            comp.append(v)
            stack.extend(adjacency[v])
        components.append(comp)

    if len(components) > 1:
        # Connect each component to the main (largest) component
        components.sort(key=len, reverse=True)
        main_set = set(components[0])
        for comp in components[1:]:
            # Pick the pair with lowest latency between comp and main
            best_u, best_v, best_lat = -1, -1, 999999
            for u in comp[:20]:  # sample to avoid O(n^2)
                for v in rng.sample(list(main_set), min(20, len(main_set))):
                    lat = latency_between(u, v)
                    if lat < best_lat:
                        best_u, best_v, best_lat = u, v, lat
            adjacency[best_u].add(best_v)
            adjacency[best_v].add(best_u)
            main_set.update(comp)

    edges = []
    for u, neighbors in adjacency.items():
        for v in neighbors:
            edges.append(Edge(source=u, target=v, latency_ms=latency_between(u, v)))
    return Topology(nodes=nodes, edges=edges)


def generate_topology(topo: "TopologyConfig") -> Topology:
    """Generate topology based on configuration."""
    if topo.file is not None:
        return load_topology(Path(topo.file))
    if topo.generate is None:
        raise ValueError("topology must specify either 'file' or 'generate'")
    gen = topo.generate
    if gen.type == "random":
        return generate_random_topology(
            num_nodes=gen.num_nodes,
            degree=gen.degree,
            seed=gen.seed,
            super_node_fraction=gen.super_node_fraction,
            origin_degree=gen.origin_degree,
        )
    elif gen.type == "ring":
        return generate_ring_topology(
            num_nodes=gen.num_nodes,
            seed=gen.seed,
            super_node_fraction=gen.super_node_fraction,
        )
    elif gen.type == "realistic":
        return generate_realistic_topology(
            num_nodes=gen.num_nodes,
            degree=gen.degree,
            seed=gen.seed,
            super_node_fraction=gen.super_node_fraction,
            origin_degree=gen.origin_degree,
            origin_country=gen.origin_country,
        )
    else:
        raise ValueError(f"Unknown topology type: {gen.type}")
