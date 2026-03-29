"""CLI entry point."""

from pathlib import Path

import click

from simctl.config import (
    Config,
    StrategyConfig,
    TopologyConfig,
    TopologyGenerate,
    load_config,
    save_config,
)
from simctl.analyze import (
    parse_run,
    print_table,
)
from simctl.paths import get_ethp2p_root
import simctl.remote as remote
from simctl.runner import run_simulation
from simctl.topology import generate_random_topology, generate_ring_topology


@click.group()
def cli() -> None:
    """Simulation control CLI for eth-ec-broadcast."""
    pass


@cli.command()
@click.option("-n", "--num-nodes", default=10, help="Number of nodes")
@click.option("-d", "--degree", default=4, help="Graph degree (connections per node)")
@click.option(
    "-t",
    "--type",
    "topo_type",
    default="random",
    type=click.Choice(["random", "ring"]),
    help="Topology type",
)
@click.option("-s", "--seed", default=42, help="Random seed for reproducibility")
@click.option("--super-node-fraction", default=0.0, help="Fraction of super nodes")
@click.option("-o", "--output", default="topology.json", help="Output file path")
def topology(
    num_nodes: int,
    degree: int,
    topo_type: str,
    seed: int,
    super_node_fraction: float,
    output: str,
) -> None:
    """Generate a network topology."""
    if topo_type == "random":
        topo = generate_random_topology(
            num_nodes=num_nodes,
            degree=degree,
            seed=seed,
            super_node_fraction=super_node_fraction,
        )
    else:
        topo = generate_ring_topology(
            num_nodes=num_nodes,
            seed=seed,
            super_node_fraction=super_node_fraction,
        )

    topo.save(Path(output))
    click.echo(f"Topology saved to {output}")


@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--output-dir", default="./runs", help="Output directory for results")
def run(config_path: str, output_dir: str) -> None:
    """Run a simulation (single or multi-strategy)."""
    config = load_config(Path(config_path))
    result = run_simulation(config, Path(output_dir))

    mode = config.simulation.driver
    if mode == "shadow" and result.run_dir.exists():
        try:
            print_table(parse_run(result.run_dir))
        except Exception as e:
            click.echo(f"Warning: could not analyze results: {e}", err=True)

    if not result.all_successful:
        raise SystemExit(1)


@cli.command()
@click.argument("results_dir", type=click.Path(exists=True))
def analyze(results_dir: str) -> None:
    """Analyze simulation results."""
    p = Path(results_dir)
    print_table(parse_run(p))


@cli.command()
@click.option("-o", "--output", default="config.yaml", help="Output file path")
@click.option("--strategy", default="RS", help="Strategy name")
def init(output: str, strategy: str) -> None:
    """Generate a default configuration file."""
    config = Config(
        strategies=[StrategyConfig(name=strategy)],
        topology=TopologyConfig(generate=TopologyGenerate()),
    )
    save_config(config, Path(output))
    click.echo(f"Default config saved to {output}")


@cli.group("remote")
def remote_command() -> None:
    """Run simctl commands on a remote host."""
    pass


@remote_command.command("run")
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--host", required=True, help="Remote host (user@hostname)")
@click.option("--output-dir", default="./runs", help="Output directory on remote")
@click.option("--dry-run", is_flag=True, help="Print commands without executing")
def remote_run(config_path: str, host: str, output_dir: str, dry_run: bool) -> None:
    """Run a simulation on a remote host.

    Syncs the codebase to remote, executes simctl run, and monitors completion.
    """
    ethp2p_root = get_ethp2p_root()
    remote_cli_dir = ethp2p_root / "sim" / "cli"
    runner = remote.Runner(dry_run=dry_run)

    click.echo(f"Syncing to {host}...")
    sync_res = runner.sync_to_remote(host, ethp2p_root)
    if sync_res.returncode != 0:
        raise SystemExit(sync_res.returncode)

    click.echo(f"Running simulation on {host}...")
    exit_code = runner.run_remote_simctl(
        host=host,
        cwd=remote_cli_dir,
        simctl_args=["run", str(config_path), "--output-dir", output_dir],
    )

    tar_rc = runner.tar_and_cleanup(host, remote_cli_dir, output_dir)
    rc = exit_code or tar_rc
    if rc != 0:
        raise SystemExit(rc)


if __name__ == "__main__":
    cli()
