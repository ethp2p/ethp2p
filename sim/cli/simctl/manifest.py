"""Manifest utilities for simctl runs."""

from __future__ import annotations

import json
import os
import secrets
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utcnow_iso() -> str:
    """Return a UTC timestamp in ISO 8601 format with milliseconds."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def format_dir_timestamp(now: datetime) -> str:
    """Format a timestamp for directory names with millisecond precision."""
    millis = now.microsecond // 1_000
    return f"{now.strftime('%Y%m%d-%H%M%S')}-{millis:03d}"


def random_suffix(num_bytes: int = 3) -> str:
    """Return a short random hex suffix for collision resistance."""
    return secrets.token_hex(num_bytes)


def try_get_git_sha(repo_root: Path) -> str | None:
    """Return the current Git SHA for repo_root, if available."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return None

    if result.returncode != 0:
        return None

    sha = result.stdout.strip()
    return sha or None


def get_command_argv() -> list[str]:
    """Return the current process argv as a list of strings."""
    return list(sys.argv)


def write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    """Write JSON to path atomically, creating parent dirs if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp_path = path.with_name(f".{path.name}.{secrets.token_hex(4)}.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.write("\n")

    os.replace(tmp_path, path)
