"""Remote execution support for simctl."""

import shlex
import subprocess
import time
from pathlib import Path
class SubprocessRunner:
    """Executes commands using subprocess."""

    def run_cmd(
        self, cmd: list[str], capture_output: bool = False, cwd: Path | None = None
    ) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(cmd, capture_output=capture_output, cwd=cwd)

    def ssh_cmd(
        self, host: str, cmd: str, capture_output: bool = False
    ) -> subprocess.CompletedProcess[bytes]:
        return self.run_cmd(
            ["ssh", "-n", "-T", host, cmd], capture_output=capture_output
        )


class DryRunRunner:
    """Prints commands without executing them."""

    def run_cmd(
        self, cmd: list[str], capture_output: bool = False, cwd: Path | None = None
    ) -> subprocess.CompletedProcess[bytes]:
        cwd_str = f" (cwd: {cwd})" if cwd else ""
        print(f"[dry-run] {' '.join(cmd)}{cwd_str}")
        return subprocess.CompletedProcess(cmd, 0, b"", b"")

    def ssh_cmd(
        self, host: str, cmd: str, capture_output: bool = False
    ) -> subprocess.CompletedProcess[bytes]:
        print(f"[dry-run] ssh {host}: {cmd}")
        if not capture_output:
            return subprocess.CompletedProcess(["ssh", host, cmd], 0, b"", b"")

        if "kill -0" in cmd:
            return subprocess.CompletedProcess(["ssh", host, cmd], 0, b"done\n", b"")

        if "cat" in cmd and ".exit" in cmd:
            return subprocess.CompletedProcess(["ssh", host, cmd], 0, b"0\n", b"")

        return subprocess.CompletedProcess(["ssh", host, cmd], 0, b"123\n", b"")


class Runner:
    """High-level remote execution runner."""

    def __init__(self, dry_run: bool):
        if dry_run:
            self._runner = DryRunRunner()
        else:
            self._runner = SubprocessRunner()

    def sync_to_remote(
        self, host: str, local_dir: Path
    ) -> subprocess.CompletedProcess[bytes]:
        """Rsync local directory to remote host.

        Respects .gitignore files in the directory tree.
        """
        parent_dir = local_dir.parent
        remote_path = str(parent_dir / local_dir.name)

        # Create remote directory structure first
        mkdir_res = self._runner.ssh_cmd(
            host, f"mkdir -p -- {shlex.quote(remote_path)}"
        )
        if mkdir_res.returncode != 0:
            return mkdir_res

        cmd = [
            "rsync",
            "-avP",
            "--exclude",
            ".git",
            "--exclude",
            ".jj",
            "--filter=:- .gitignore",  # Respect .gitignore files in each directory
            str(local_dir.name) + "/",
            f"{host}:{shlex.quote(remote_path + '/')}",
        ]
        return self._runner.run_cmd(cmd, cwd=parent_dir)

    def run_remote_simctl(
        self,
        host: str,
        cwd: Path,
        simctl_args: list[str],
        log_path: str = "/tmp/simctl-remote.log",
        poll_interval: int = 10,
    ) -> int:
        """Run simctl on remote host and poll for completion.

        Args:
            host: Remote host in user@hostname format
            cwd: Working directory on remote (e.g., ethp2p/sim/cli)
            simctl_args: Arguments to pass to simctl command
            log_path: Path on remote host for log output
            poll_interval: Seconds between status checks

        Returns:
            Exit code from the remote simctl process
        """
        # Start simctl in background using uv run (sync first to install deps)
        # Wrap in bash -lc to ensure consistent shell behavior regardless of remote login shell
        args_str = " ".join(shlex.quote(arg) for arg in simctl_args)

        log_path_resolved = Path(log_path)
        if not log_path_resolved.is_absolute():
            log_path_resolved = cwd / log_path_resolved

        log_path_remote = str(log_path_resolved)
        exit_code_path = f"{log_path_remote}.exit"

        inner_script = (
            f"cd {shlex.quote(str(cwd))} && "
            f"uv run simctl {args_str}; "
            f"rc=$?; "
            f'printf %s "$rc" > {shlex.quote(exit_code_path)}; '
            f'exit "$rc"'
        )
        start_script = (
            f"set -euo pipefail; "
            f"mkdir -p -- {shlex.quote(str(log_path_resolved.parent))}; "
            f"cd {shlex.quote(str(cwd))}; "
            f"uv sync --quiet 1>&2; "
            f"rm -f -- {shlex.quote(exit_code_path)}; "
            f"nohup bash -lc {shlex.quote(inner_script)} "
            f"> {shlex.quote(log_path_remote)} 2>&1 & "
            f"echo $!"
        )

        remote_cmd = f"bash -lc {shlex.quote(start_script)}"
        result = self._runner.ssh_cmd(host, remote_cmd, capture_output=True)
        if result.returncode != 0:
            return result.returncode

        pid = result.stdout.decode().strip()
        if not pid.isdigit():
            print(f"Failed to parse remote PID from output: {pid!r}")
            return 1

        print(f"Started remote process with PID {pid}")

        # Poll for completion
        while True:
            check = self._runner.ssh_cmd(
                host,
                f"bash -lc {shlex.quote(f'kill -0 {pid} 2>/dev/null && echo running || echo done')}",
                capture_output=True,
            )
            if check.returncode != 0:
                return check.returncode

            status = check.stdout.decode().strip()
            if status == "done":
                break
            print(f"Process {pid} still running...")
            time.sleep(poll_interval)

        # Show logs
        log_res = self._runner.ssh_cmd(
            host, f"cat {shlex.quote(log_path_remote)}"
        )
        if log_res.returncode != 0:
            print(
                f"Failed to read remote log {log_path_remote!r} (exit code {log_res.returncode})"
            )

        exit_res = self._runner.ssh_cmd(
            host, f"cat {shlex.quote(exit_code_path)}", capture_output=True
        )
        if exit_res.returncode != 0:
            return exit_res.returncode

        exit_code_raw = exit_res.stdout.decode().strip()
        try:
            exit_code = int(exit_code_raw)
        except ValueError:
            print(
                f"Failed to parse remote exit code from {exit_code_path!r}: {exit_code_raw!r}"
            )
            return 1

        return exit_code

    def tar_and_cleanup(self, host: str, cwd: Path, output_dir: str) -> int:
        """Tar a remote output directory and remove the original.

        Creates {output_dir}.tar.gz in the parent directory, then removes
        the output directory.

        Args:
            host: Remote host in user@hostname format
            cwd: Working directory for resolving relative output_dir
            output_dir: Output directory path (absolute, or relative to cwd)

        Returns:
            0 on success, or a non-zero exit code on failure
        """
        # Tar the output directory to parent folder as {dirname}.tar.gz
        # e.g., /tmp/experiment-run -> /tmp/experiment-run.tar.gz
        output_path = Path(output_dir)
        if not output_path.is_absolute():
            output_path = cwd / output_path
        parent_dir = output_path.parent

        tar_path = str(parent_dir / f"{output_path.name}.tar.gz")
        tar_script = (
            f"tar -czf {shlex.quote(tar_path)} "
            f"-C {shlex.quote(str(parent_dir))} "
            f"{shlex.quote(output_path.name)}"
        )
        print(f"Creating tarball {tar_path}...")
        tar_res = self._runner.ssh_cmd(
            host, f"bash -lc {shlex.quote(tar_script)}"
        )
        if tar_res.returncode != 0:
            print(
                f"Failed to create tarball {tar_path!r} (exit code {tar_res.returncode})"
            )
            return tar_res.returncode

        print(f"Tarball created at {tar_path}")

        # Remove the output directory after tarring
        rm_cmd = f"rm -rf -- {shlex.quote(str(output_path))}"
        rm_res = self._runner.ssh_cmd(
            host, f"bash -lc {shlex.quote(rm_cmd)}"
        )
        if rm_res.returncode != 0:
            print(
                f"Failed to remove {output_path!r} (exit code {rm_res.returncode})"
            )
            return rm_res.returncode

        print(f"Removed {output_path}")

        return 0
