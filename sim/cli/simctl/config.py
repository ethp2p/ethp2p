"""Configuration schema and validation."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, model_validator


class SimulationConfig(BaseModel):
    """Runtime/infrastructure settings."""
    driver: Literal["shadow", "simnet"] = "shadow"
    log_level: Literal["debug", "info", "warn", "error"] = "info"
    bandwidth_log_frequency_ms: int = 100
    trace_file: str | None = None


class StrategyConfig(BaseModel):
    """Strategy-specific configuration."""
    name: Literal["RS", "RLNC", "gossipsub"]

    # RS options
    data_shards: int = 16
    parity_shards: int = 16

    # RS fixed-size chunk option.
    chunk_len: int | None = None

    # RLNC options
    num_chunks: int = 16
    num_chunks_per_generation: int = 0
    target_chunk_size: int = 0
    origin_redundancy: int = 0

    # Forward multiplier (RS and RLNC relays)
    forward_multiplier: int = 4

    # Bitmap options
    enable_bitmaps: bool = True
    bitmap_threshold: int = 100

    # Per-strategy override for publish wait (optional, used in experiments)
    publish_wait_seconds: float | None = None

    @model_validator(mode="after")
    def validate_strategy_fields(self) -> "StrategyConfig":
        if self.name == "RLNC" and "chunk_len" in self.model_fields_set:
            raise ValueError("RLNC uses target_chunk_size, not chunk_len")
        if (
            self.name == "RLNC"
            and self.target_chunk_size > 0
            and self.num_chunks_per_generation <= 0
        ):
            raise ValueError(
                "RLNC target_chunk_size requires num_chunks_per_generation > 0"
            )
        return self


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

    if strat.name == "RS":
        parts.append(f"d{strat.data_shards}")
        parts.append(f"p{strat.parity_shards}")
        if strat.chunk_len is not None:
            parts.append(f"cl{strat.chunk_len}")

    if strat.name == "RLNC":
        parts.append(f"nc{strat.num_chunks}")
        if strat.target_chunk_size:
            parts.append(f"tcs{strat.target_chunk_size}")
        if strat.num_chunks_per_generation:
            parts.append(f"ncpg{strat.num_chunks_per_generation}")
        if strat.origin_redundancy:
            parts.append(f"or{strat.origin_redundancy}")

    if strat.name == "RS":
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
    d["strategy"] = strategy_for_go(strat)
    return d


def strategy_for_go(strat: StrategyConfig) -> dict:
    """Return only the fields consumed by the selected Go strategy."""
    out: dict[str, object] = {"name": strat.name}

    if strat.name == "RS":
        out.update(
            data_shards=strat.data_shards,
            parity_shards=strat.parity_shards,
            forward_multiplier=strat.forward_multiplier,
            enable_bitmaps=strat.enable_bitmaps,
            bitmap_threshold=strat.bitmap_threshold,
        )
        if strat.chunk_len is not None:
            out["chunk_len"] = strat.chunk_len
    elif strat.name == "RLNC":
        out.update(
            num_chunks=strat.num_chunks,
            num_chunks_per_generation=strat.num_chunks_per_generation,
            target_chunk_size=strat.target_chunk_size,
            origin_redundancy=strat.origin_redundancy,
            forward_multiplier=strat.forward_multiplier,
        )

    return out
