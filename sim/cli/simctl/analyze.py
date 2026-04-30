"""Parse Shadow/simnet run results and print comparison tables."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class MessageStats:
    """Per-message stats for a single strategy run."""

    delivered: int = 0
    latencies_ms: list[float] = field(default_factory=list)

    # Per-relay bandwidth for this message (sorted).
    relay_sent_vals: list[int] = field(default_factory=list)
    relay_recv_vals: list[int] = field(default_factory=list)
    origin_sent: int = 0
    origin_received: int = 0

    # verdict_name → sorted per-relay values (order preserved from log)
    relay_verdicts: dict[str, list[int]] = field(default_factory=dict)


@dataclass
class RunStats:
    """Parsed stats from a single simulation run."""

    name: str
    num_nodes: int = 0
    message_size: int = 0
    expected: int = 0

    # msg_idx → per-message stats
    messages: dict[int, MessageStats] = field(default_factory=dict)


_PUB_RE = re.compile(
    r'msg="Published Message" node-num=\d+ message-id=(\S+) at=(\d+)'
)
_RECV_RE = re.compile(
    r'msg="Received Message" node-num=(\d+) message-id=(\S+) at=(\d+)'
)
_MSG_STATS_RE = re.compile(
    r'msg="message stats" node-num=(\d+) message-id=(\S+)\s+(.*)'
)
_KV_RE = re.compile(r"(\S+?)=(\d+)")
_VERDICT_NAMES = {
    0: "accepted",
    1: "redundant",
    2: "decoding",
    3: "surplus",
}


def _msg_idx(message_id: str) -> int:
    return int(message_id.removeprefix("msg-"))


def _parse_trace_chunk_stats(node_dirs: list[Path]) -> dict[int, dict[int, dict[str, int]]]:
    """Parse exact per-message chunk verdicts from per-node trace files."""
    stats: dict[int, dict[int, dict[str, int]]] = {}

    for node_dir in node_dirs:
        events_file = node_dir / "events.ndjson"
        if not events_file.exists():
            continue

        for line in events_file.read_text().splitlines():
            if not line.startswith("["):
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            if len(ev) < 7 or ev[2] != "cr":
                continue
            verdict_name = _VERDICT_NAMES.get(ev[6])
            if verdict_name is None:
                continue
            node_num = int(ev[1])
            idx = _msg_idx(ev[5])
            msg_node_stats = stats.setdefault(idx, {}).setdefault(node_num, {})
            msg_node_stats[verdict_name] = msg_node_stats.get(verdict_name, 0) + 1

    return stats


def parse_shadow_run(run_dir: Path, name: str, message_size: int) -> RunStats:
    """Parse a Shadow run directory for bandwidth, latency, and chunk stats."""
    hosts_dir = run_dir / "shadow.data" / "hosts"
    if not hosts_dir.exists():
        return RunStats(name=name)

    node_dirs = sorted(
        hosts_dir.iterdir(),
        key=lambda p: int(p.name.removeprefix("node")),
    )

    publish_times: dict[int, int] = {}
    receive_times: dict[int, dict[int, int]] = {}
    # msg_idx → node_num → {key: value} from "message stats" lines
    msg_stats: dict[int, dict[int, dict[str, int]]] = {}

    for node_dir in node_dirs:
        stderr_file = node_dir / "simnode.1000.stderr"
        if not stderr_file.exists():
            continue

        for line in stderr_file.read_text().splitlines():
            if m := _PUB_RE.search(line):
                idx = _msg_idx(m.group(1))
                publish_times[idx] = int(m.group(2))
            elif m := _RECV_RE.search(line):
                nn = int(m.group(1))
                idx = _msg_idx(m.group(2))
                receive_times.setdefault(idx, {})[nn] = int(m.group(3))
            elif m := _MSG_STATS_RE.search(line):
                nn = int(m.group(1))
                idx = _msg_idx(m.group(2))
                kvs: dict[str, int] = {}
                for kv in _KV_RE.finditer(m.group(3)):
                    kvs[kv.group(1)] = int(kv.group(2))
                msg_stats.setdefault(idx, {})[nn] = kvs

    trace_chunk_stats = _parse_trace_chunk_stats(node_dirs)
    for idx, by_node in trace_chunk_stats.items():
        for nn, chunk_stats in by_node.items():
            msg_stats.setdefault(idx, {}).setdefault(nn, {}).update(chunk_stats)

    num_nodes = len(node_dirs)
    expected = num_nodes - 1

    # Known bandwidth keys emitted by logMessageStats; everything else
    # is a verdict name. Collect verdict names preserving first-seen order.
    bw_keys = {"sentBytesTotal", "receivedBytesTotal"}
    verdict_order: list[str] = []
    verdict_seen: set[str] = set()
    for idx in sorted(msg_stats):
        for nn in sorted(msg_stats[idx]):
            for k in msg_stats[idx][nn]:
                if k not in bw_keys and k not in verdict_seen:
                    verdict_seen.add(k)
                    verdict_order.append(k)

    messages: dict[int, MessageStats] = {}
    for idx in sorted(publish_times):
        pub_t = publish_times[idx]
        lats = sorted(
            rt - pub_t for _, rt in sorted(receive_times.get(idx, {}).items())
        )

        per_node = msg_stats.get(idx, {})
        relay_nodes = sorted(k for k in per_node if k != 0)

        relay_sent = sorted(per_node.get(k, {}).get("sentBytesTotal", 0) for k in relay_nodes)
        relay_recv = sorted(per_node.get(k, {}).get("receivedBytesTotal", 0) for k in relay_nodes)
        origin_kv = per_node.get(0, {})

        relay_verdicts: dict[str, list[int]] = {}
        for vname in verdict_order:
            relay_verdicts[vname] = sorted(
                per_node.get(k, {}).get(vname, 0) for k in relay_nodes
            )

        messages[idx] = MessageStats(
            delivered=len(receive_times.get(idx, {})),
            latencies_ms=lats,
            relay_sent_vals=relay_sent,
            relay_recv_vals=relay_recv,
            origin_sent=origin_kv.get("sentBytesTotal", 0),
            origin_received=origin_kv.get("receivedBytesTotal", 0),
            relay_verdicts=relay_verdicts,
        )

    return RunStats(
        name=name,
        num_nodes=num_nodes,
        message_size=message_size,
        expected=expected,
        messages=messages,
    )


def parse_run(run_dir: Path) -> list[RunStats]:
    """Parse all strategy results in a run directory.

    Detects three layouts:
    - strategies/ subdir (new unified layout)
    - runs/ subdir (legacy experiment layout)
    - shadow.data directly in run_dir (legacy single run)
    """
    import yaml

    strategies_dir = run_dir / "strategies"
    if strategies_dir.exists():
        results: list[RunStats] = []
        for strat_dir in sorted(strategies_dir.iterdir()):
            if not strat_dir.is_dir():
                continue
            config_path = strat_dir / "run.yaml"
            if not config_path.exists():
                config_path = strat_dir / "config.yaml"
            if not config_path.exists():
                continue
            with open(config_path) as f:
                cfg = yaml.safe_load(f)
            strat_name = cfg.get("strategy", {}).get("name", "unknown")
            msg_size = cfg.get("workload", {}).get("message_size", 0)
            results.append(parse_shadow_run(strat_dir, strat_name, msg_size))
        return results

    runs_dir = run_dir / "runs"
    if runs_dir.exists():
        results = []
        for rd in sorted(runs_dir.iterdir()):
            if not rd.is_dir():
                continue
            config_path = rd / "config.yaml"
            if not config_path.exists():
                continue
            with open(config_path) as f:
                cfg = yaml.safe_load(f)
            strat_name = cfg.get("strategy", {}).get("name", "unknown")
            msg_size = cfg.get("workload", {}).get("message_size", 0)
            results.append(parse_shadow_run(rd, strat_name, msg_size))
        return results

    if (run_dir / "shadow.data").exists():
        config_path = run_dir / "config.yaml"
        if not config_path.exists():
            raise FileNotFoundError(f"No config.yaml in {run_dir}")
        with open(config_path) as f:
            cfg = yaml.safe_load(f)
        strat_name = cfg.get("strategy", {}).get("name", "unknown")
        msg_size = cfg.get("workload", {}).get("message_size", 0)
        return [parse_shadow_run(run_dir, strat_name, msg_size)]

    raise FileNotFoundError(f"Cannot find strategies/, runs/, or shadow.data in {run_dir}")


def _pct(sorted_vals: list, p: float) -> float:
    if not sorted_vals:
        return 0.0
    return sorted_vals[min(int(len(sorted_vals) * p), len(sorted_vals) - 1)]


def _human_bytes(b: float) -> str:
    b = int(b)
    if b >= 1 << 30:
        return f"{b / (1 << 30):.1f} GiB"
    if b >= 1 << 20:
        return f"{b / (1 << 20):.1f} MiB"
    if b >= 1 << 10:
        return f"{b / (1 << 10):.1f} KiB"
    return f"{b} B"


def _ms(v: float) -> str:
    return f"{v:.0f} ms"


def _int_str(v: float) -> str:
    return str(int(v))


def _empty_msg() -> MessageStats:
    return MessageStats()


def _get_msg(r: RunStats, idx: int) -> MessageStats:
    return r.messages.get(idx, _empty_msg())


def _sanity_checks(results: list[RunStats], msg_size: int) -> list[tuple[bool, str]]:
    """Run sanity checks across all results, returning (passed, label) pairs."""
    checks: list[tuple[bool, str]] = []

    for r in results:
        if r.expected <= 0:
            continue
        multi = len(r.messages) > 1
        for idx in sorted(r.messages):
            ms = r.messages[idx]
            ok = ms.delivered >= r.expected
            suffix = f" [{idx}]" if multi else ""
            checks.append((ok, (
                f"Full delivery: {r.name}{suffix} "
                f"delivered {ms.delivered}/{r.expected}"
            )))

    return checks


def _msg_indices(results: list[RunStats]) -> list[int]:
    """Return the sorted union of message indices across all results."""
    indices: set[int] = set()
    for r in results:
        indices.update(r.messages)
    return sorted(indices)


def print_table(results: list[RunStats]) -> None:
    """Print the comparison table for one or more runs."""
    if not results:
        return

    r0 = results[0]
    msg_size = r0.message_size or 1

    print(f"\n{'=' * 80}")
    print(f"  {r0.num_nodes} nodes, {_human_bytes(msg_size)} payload")
    print(f"{'=' * 80}")

    pcts = [0.5, 0.8, 0.95, 0.99]
    pct_labels = ["p50", "p80", "p95", "p99"]
    lw = 22

    msg_indices = _msg_indices(results)
    multi_msg = len(msg_indices) > 1

    # One column per (strategy, msg_idx). All rows use this layout.
    cols: list[tuple[RunStats, int]] = []
    names: list[str] = []
    for r in results:
        for idx in msg_indices:
            cols.append((r, idx))
            names.append(f"{r.name} [{idx}]" if multi_msg else r.name)

    col_w = max(max((len(n) for n in names), default=10) + 2, 12)

    def hdr() -> str:
        return f"{'':>{lw}}" + "".join(f"{n:>{col_w}}" for n in names)

    def sep() -> str:
        return f"{'─' * lw}" + "".join(f" {'─' * (col_w - 1)}" for _ in names)

    def row(label: str, vals: list[str]) -> str:
        return f"{label:>{lw}}" + "".join(f"{v:>{col_w}}" for v in vals)

    def ms(r: RunStats, idx: int) -> MessageStats:
        return _get_msg(r, idx)

    print(f"\n{hdr()}")
    print(sep())

    print(row("delivery", [
        f"{ms(r, idx).delivered}/{r.expected}" for r, idx in cols
    ]))
    print(row("multiplier", [
        f"{(ms(r, idx).origin_sent + sum(ms(r, idx).relay_sent_vals)) / msg_size:.0f}x"
        for r, idx in cols
    ]))

    print(sep())
    for i, pl in enumerate(pct_labels):
        print(row(f"latency {pl}", [
            _ms(_pct(ms(r, idx).latencies_ms, pcts[i]))
            for r, idx in cols
        ]))

    print(sep())
    print(row("origin upload", [
        _human_bytes(ms(r, idx).origin_sent) for r, idx in cols
    ]))
    print(row("origin download", [
        _human_bytes(ms(r, idx).origin_received) for r, idx in cols
    ]))

    print(sep())
    for i, pl in enumerate(pct_labels):
        print(row(f"relay upload {pl}", [
            _human_bytes(_pct(ms(r, idx).relay_sent_vals, pcts[i]))
            for r, idx in cols
        ]))

    print(sep())
    for i, pl in enumerate(pct_labels):
        print(row(f"relay download {pl}", [
            _human_bytes(_pct(ms(r, idx).relay_recv_vals, pcts[i]))
            for r, idx in cols
        ]))

    print(sep())
    for i, pl in enumerate(pct_labels):
        print(row(f"relay up mult {pl}", [
            f"{_pct(ms(r, idx).relay_sent_vals, pcts[i]) / msg_size:.1f}x"
            for r, idx in cols
        ]))

    print(sep())
    for i, pl in enumerate(pct_labels):
        print(row(f"relay down mult {pl}", [
            f"{_pct(ms(r, idx).relay_recv_vals, pcts[i]) / msg_size:.1f}x"
            for r, idx in cols
        ]))

    all_verdict_names: list[str] = []
    seen: set[str] = set()
    for r in results:
        for m in r.messages.values():
            for vname in m.relay_verdicts:
                if vname not in seen:
                    seen.add(vname)
                    all_verdict_names.append(vname)

    for vname in all_verdict_names:
        print(sep())
        for i, pl in enumerate(pct_labels):
            vvals = [ms(r, idx).relay_verdicts.get(vname, []) for r, idx in cols]
            print(row(f"chunks {vname} {pl}", [
                _int_str(_pct(v, pcts[i])) if v else "n/a" for v in vvals
            ]))

    checks = _sanity_checks(results, msg_size)
    if checks:
        print(f"\n{'─' * 80}")
        print("  Sanity checks")
        print(f"{'─' * 80}")
        for passed, label in checks:
            mark = "v" if passed else "x"
            print(f"  [{mark}]  {label}")

    print()
