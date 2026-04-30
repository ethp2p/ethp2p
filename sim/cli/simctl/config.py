"""Configuration schema and validation."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel


class SimulationConfig(BaseModel):
    """Runtime/infrastructure settings."""
    driver: Literal["shadow", "simnet"] = "shadow"
    log_level: Literal["debug", "info", "warn", "error"] = "info"
    bandwidth_log_frequency_ms: int = 100
    trace_file: str | None = None


class StrategyConfig(BaseModel):
    """Strategy-specific configuration."""
    name: Literal["RS", "RS-ChunkLen", "gossipsub"]

    # RS options
    data_shards: int = 16
    parity_shards: int = 16

    # Chunk options (RS-ChunkLen)
    chunk_len: int | None = None

    # Forward multiplier (RS relays)
    forward_multiplier: int = 4

    # Bitmap options
    enable_bitmaps: bool = True
    bitmap_threshold: int = 100

    # Per-strategy override for publish wait (optional, used in experiments)
    publish_wait_seconds: float | None = None


class WorkloadConfig(BaseModel):
    """What to publish: message count, size, pacing, duration."""
    num_messages: int = 1
    message_size: int = 100_000
    publish_wait_seconds: float = 10.0
    stop_time_minutes: float = 30.0
    warmup: Literal["auto", "off"] = "auto"


class TopologyGenerate(BaseModel):
    """Parameters for Python-side topology generation."""
    num_nodes: int = 10
    degree: int = 4
    origin_degree: int = 0  # 0 means same as degree
    type: Literal["random", "ring", "realistic"] = "random"
    origin_country: str = "united states"
    seed: int = 42
    super_node_fraction: float = 0.0


class TopologyConfig(BaseModel):
    """Topology configuration with two mutually exclusive modes."""
    file: str | None = None
    generate: TopologyGenerate | None = None


class Config(BaseModel):
    """Root configuration for a simulation run."""
    name: str = ""
    description: str = ""
    simulation: SimulationConfig = SimulationConfig()
    strategies: list[StrategyConfig]
    workload: WorkloadConfig = WorkloadConfig()
    topology: TopologyConfig


def load_config(path: Path) -> Config:
    """Load and validate configuration from YAML file."""
    import yaml
    with open(path) as f:
        data = yaml.safe_load(f)
    if "strategy" in data and "strategies" not in data:
        data["strategies"] = [data.pop("strategy")]
    return Config(**data)


def save_config(config: Config, path: Path) -> None:
    """Save configuration to YAML file."""
    import yaml
    with open(path, "w") as f:
        yaml.dump(config.model_dump(exclude_none=True), f, default_flow_style=False)


def get_strategy_dir_name(strat: StrategyConfig, num_nodes: int, msg_size: int) -> str:
    """Generate a compact directory name for a strategy run."""
    parts = [strat.name]

    if strat.name in ("RS", "RS-ChunkLen"):
        parts.append(f"d{strat.data_shards}")
        parts.append(f"p{strat.parity_shards}")

    if strat.name == "RS-ChunkLen" and strat.chunk_len is not None:
        parts.append(f"cl{strat.chunk_len}")

    if strat.name != "gossipsub":
        parts.append(f"bm{int(strat.enable_bitmaps)}")
        parts.append(f"t{strat.bitmap_threshold}")

    parts.append(f"n{num_nodes}")
    parts.append(str(msg_size))

    return "-".join(parts)


def resolve_for_go(config: Config, strat: StrategyConfig) -> dict:
    """Resolve a unified config into a Go-consumable dict with singular strategy."""
    d = config.model_dump(exclude_none=True)
    d.pop("strategies", None)
    d.pop("name", None)
    d.pop("description", None)
    d["strategy"] = strat.model_dump(exclude_none=True)
    return d
