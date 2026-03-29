"""Path utilities for simctl."""

from pathlib import Path


def get_ethp2p_root() -> Path:
    """Get the ethp2p root directory.

    Assumes simctl package is at ethp2p/sim/cli/simctl/.
    """
    # __file__ is paths.py -> parent is simctl/ -> cli/ -> sim/ -> ethp2p/
    return Path(__file__).parent.parent.parent.parent.resolve()
