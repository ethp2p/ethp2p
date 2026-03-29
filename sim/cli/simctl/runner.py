"""Simulation runners for simnet and Shadow modes."""

import gzip
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from simctl.config import (
    Config,
    StrategyConfig,
    get_strategy_dir_name,
    resolve_for_go,
    save_config,
)
from simctl.manifest import (
    format_dir_timestamp,
    get_command_argv,
    random_suffix,
    try_get_git_sha,
    utcnow_iso,
    write_json_atomic,
)
from simctl.paths import get_ethp2p_root
from simctl.topology import Topology, generate_topology
from simctl.trace_merge import merge_shadow_traces


@dataclass
class StrategyResult:
    strategy_name: str
    strategy_dir: Path
    exit_code: int


@dataclass
class RunResult:
    run_dir: Path
    strategies: list[StrategyResult]

    @property
    def all_successful(self) -> bool:
        return all(r.exit_code == 0 for r in self.strategies)


def get_run_dir(config: Config, base_dir: Path, mode: str) -> Path:
    """Generate a unique run directory name based on config and timestamp."""
    now = datetime.now()
    timestamp = format_dir_timestamp(now)
    suffix = random_suffix()
    safe_name = config.name.replace(" ", "-").lower() if config.name else "run"
    return base_dir / f"run-{mode}-{timestamp}-{suffix}-{safe_name}"


def build_simnode(output_path: Path) -> None:
    """Build the shadow node Go binary."""
    sim_dir = Path(__file__).parent.parent.parent.resolve()
    simnode_src = sim_dir / "cmd" / "shadow"
    cmd = [
        "go",
        "build",
        "-buildvcs=false",
        "-o",
        str(output_path.resolve()),
        str(simnode_src),
    ]
    subprocess.run(cmd, check=True)


def build_simnet_test(output_path: Path) -> None:
    """Compile the simnet test binary."""
    cmd = [
        "go",
        "test",
        "-c",
        "-o",
        str(output_path.resolve()),
        "./sim/cmd/simnet",
    ]
    subprocess.run(cmd, check=True, cwd=get_ethp2p_root())


def write_resolved_config(
    config: Config, strat: StrategyConfig, topology_path: str, path: Path
) -> None:
    """Write a resolved run config with topology.file set.

    Replaces topology.generate with topology.file pointing to the
    generated topology JSON, and flattens the strategy list into a
    singular strategy dict so Go can load it directly.
    """
    resolved = resolve_for_go(config, strat)
    resolved["topology"] = {"file": topology_path}
    with open(path, "w") as f:
        yaml.dump(resolved, f, default_flow_style=False, sort_keys=False)


# ---------------------------------------------------------------------------
# Shadow YAML / GML generation (merged from shadow.py)
# ---------------------------------------------------------------------------


def generate_gml(topology: Topology) -> str:
    """Generate GML graph from topology for Shadow network simulation."""
    lines = ["graph [", "  directed 0"]

    for node in topology.nodes:
        lines.extend(
            [
                "  node [",
                f"    id {node.num}",
                f'    host_bandwidth_up "{node.upload_bw_mbps} Mbit"',
                f'    host_bandwidth_down "{node.download_bw_mbps} Mbit"',
                "  ]",
            ]
        )

    for node in topology.nodes:
        lines.extend(
            [
                "  edge [",
                f"    source {node.num}",
                f"    target {node.num}",
                '    latency "1 ms"',
                "    packet_loss 0.0",
                "  ]",
            ]
        )

    for edge in topology.edges:
        lines.extend(
            [
                "  edge [",
                f"    source {edge.source}",
                f"    target {edge.target}",
                f'    latency "{edge.latency_ms} ms"',
                "    packet_loss 0.0",
                "  ]",
            ]
        )

    lines.append("]")
    return "\n".join(lines)


def generate_shadow_yaml(
    topology: Topology,
    binary_path: str,
    config_path: str,
    config: Config,
) -> dict[str, Any]:
    """Generate Shadow configuration dictionary."""
    gml = generate_gml(topology)

    hosts: dict[str, Any] = {}
    for node in topology.nodes:
        node_args = f"--config={config_path} --node-num={node.num}"
        hosts[f"node{node.num}"] = {
            "network_node_id": node.num,
            "processes": [
                {
                    "path": binary_path,
                    "args": node_args,
                    "start_time": "0 sec",
                }
            ],
        }

    stop_minutes = int(config.workload.stop_time_minutes)

    return {
        "general": {
            "stop_time": f"{stop_minutes} min",
            "log_level": config.simulation.log_level,
            "progress": True,
            "heartbeat_interval": "10s",
        },
        "network": {
            "graph": {
                "type": "gml",
                "inline": gml,
            },
            "use_shortest_path": True,
        },
        "hosts": hosts,
    }


def write_shadow_config(config_dict: dict[str, Any], path: Path) -> None:
    """Write Shadow configuration to YAML file."""
    with open(path, "w") as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)


def run_shadow_with_topology(
    config: Config,
    strat: StrategyConfig,
    topology: Topology,
    run_dir: Path,
    parent_topology_path: Path,
) -> subprocess.CompletedProcess[bytes]:
    """Run Shadow simulation with a pre-generated topology."""
    run_dir = run_dir.resolve()

    run_config_path = "run.yaml"
    # Shadow sets each process's CWD to shadow.data/hosts/<hostname>/,
    # so the resolved config must use an absolute topology path.
    write_resolved_config(
        config, strat, str(parent_topology_path.resolve()), run_dir / run_config_path
    )

    binary_path = run_dir / "simnode"
    build_simnode(binary_path)

    shadow_config = generate_shadow_yaml(
        topology=topology,
        binary_path="./simnode",
        config_path=str(run_dir / run_config_path),
        config=config,
    )
    write_shadow_config(shadow_config, run_dir / "shadow.yaml")

    log_path = run_dir / "shadow.log"
    with open(log_path, "w") as log_file:
        result = subprocess.run(
            ["shadow", "shadow.yaml"],
            cwd=run_dir,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )

    print(f"Shadow completed. Results in: {run_dir}")
    print(f"Exit code: {result.returncode}")

    if config.simulation.trace_file and result.returncode == 0:
        trace_out = run_dir / config.simulation.trace_file
        n = merge_shadow_traces(
            shadow_data=run_dir / "shadow.data",
            topology=topology,
            strat=strat,
            output=trace_out,
        )
        print(f"Trace merged: {n} events -> {trace_out}")

    return result


def run_simnet_with_topology(
    config: Config,
    strat: StrategyConfig,
    topology: Topology,
    run_dir: Path,
    parent_topology_path: Path,
) -> subprocess.CompletedProcess[bytes]:
    """Run simnet simulation with a pre-generated topology."""
    run_config_path = "run.yaml"
    write_resolved_config(
        config, strat, str(parent_topology_path), run_dir / run_config_path
    )

    binary_path = run_dir / "simnetnode.test"
    build_simnet_test(binary_path)

    timeout_minutes = int(config.workload.stop_time_minutes) + 5

    cmd = [
        "./simnetnode.test",
        "-test.v",
        "-test.count=1",
        f"-test.timeout={timeout_minutes}m",
        "-test.run=TestScenario",
        f"--config={run_config_path}",
    ]

    log_path = run_dir / "simnet.log"
    with open(log_path, "w") as log_file:
        result = subprocess.run(
            cmd,
            cwd=run_dir,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )

    print(f"Simnet completed. Results in: {run_dir}")
    print(f"Exit code: {result.returncode}")

    return result


def run_with_topology(
    mode: str,
    config: Config,
    strat: StrategyConfig,
    topology: Topology,
    run_dir: Path,
    parent_topology_path: Path,
) -> subprocess.CompletedProcess[bytes]:
    """Run simulation with a pre-generated topology, dispatching by mode."""
    if mode == "simnet":
        return run_simnet_with_topology(
            config, strat, topology, run_dir, parent_topology_path
        )
    elif mode == "shadow":
        return run_shadow_with_topology(
            config, strat, topology, run_dir, parent_topology_path
        )
    else:
        raise ValueError(f"Unknown mode: {mode!r}")


def run_simulation(config: Config, output_dir: Path) -> RunResult:
    """Run all strategies in a config, sharing one topology."""
    started_at = utcnow_iso()
    git_sha = try_get_git_sha(get_ethp2p_root())
    mode = config.simulation.driver

    topology = generate_topology(config.topology)

    run_dir: Path | None = None
    for _ in range(20):
        candidate = get_run_dir(config, output_dir, mode)
        try:
            candidate.mkdir(parents=True, exist_ok=False)
        except FileExistsError:
            continue
        run_dir = candidate
        break

    if run_dir is None:
        raise RuntimeError("Failed to create a unique run directory after 20 attempts")

    topology.save(run_dir / "topology.json")
    save_config(config, run_dir / "config.yaml")

    manifest_path = run_dir / "manifest.json"
    manifest: dict[str, object] = {
        "schema_version": 1,
        "kind": "run",
        "status": "running",
        "started_at": started_at,
        "mode": mode,
        "command": get_command_argv(),
        "cwd": str(Path.cwd()),
        "git_sha": git_sha,
        "paths": {"run_dir": str(run_dir.resolve())},
        "params": config.model_dump(),
        "strategies": [],
    }
    write_json_atomic(manifest_path, manifest)

    strategies_dir = run_dir / "strategies"
    results: list[StrategyResult] = []

    for i, strat in enumerate(config.strategies, 1):
        if len(config.strategies) > 1:
            print(f"\n[{i}/{len(config.strategies)}] Running {strat.name}...")

        strat_dir_name = get_strategy_dir_name(
            strat, len(topology.nodes), config.workload.message_size
        )
        strat_dir = strategies_dir / strat_dir_name
        strat_dir.mkdir(parents=True, exist_ok=True)

        result = run_with_topology(
            mode=mode,
            config=config,
            strat=strat,
            topology=topology,
            run_dir=strat_dir,
            parent_topology_path=run_dir / "topology.json",
        )

        results.append(
            StrategyResult(
                strategy_name=strat.name,
                strategy_dir=strat_dir,
                exit_code=result.returncode,
            )
        )

        strat_summary = {
            "strategy_name": strat.name,
            "strategy_dir": str(strat_dir.relative_to(run_dir)),
            "exit_code": result.returncode,
        }
        strategies_list = list(manifest.get("strategies", []))
        strategies_list.append(strat_summary)
        manifest["strategies"] = strategies_list
        write_json_atomic(manifest_path, manifest)

    manifest["status"] = "completed"
    manifest["completed_at"] = utcnow_iso()
    write_json_atomic(manifest_path, manifest)

    if config.simulation.trace_file:
        _collect_traces(config.simulation.trace_file, strategies_dir, run_dir / "traces")

    if len(config.strategies) > 1:
        print(f"\n{'=' * 50}")
        print("Run completed!")
        print(f"Results directory: {run_dir}")
        for r in results:
            status = "OK" if r.exit_code == 0 else f"FAILED (exit code {r.exit_code})"
            print(f"  {r.strategy_name}: {status}")

    return RunResult(run_dir=run_dir, strategies=results)


def _collect_traces(trace_filename: str, strategies_dir: Path, dest: Path) -> None:
    """Gzip each strategy's trace file into a single traces/ directory."""
    trace_files = sorted(strategies_dir.rglob(trace_filename))
    if not trace_files:
        return

    dest.mkdir(parents=True, exist_ok=True)
    for tf in trace_files:
        strat_name = tf.parent.name
        gz_path = dest / f"{strat_name}.{trace_filename}.gz"
        with open(tf, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        tf.unlink()
        print(f"Trace compressed: {gz_path.name}")
