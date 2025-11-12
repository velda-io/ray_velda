import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from ray.autoscaler.command_runner import CommandRunnerInterface
import subprocess
import os

logger = logging.getLogger(__name__)


class VeldaCommandRunner(CommandRunnerInterface):
    """
    Command runner for vrun-based nodes.
    Since vrun handles remote execution, this is mostly empty.
    """

    def __init__(self, log_prefix: str, node_id: str, provider, auth_config: Dict[str, Any],
                 cluster_name: str, process_runner, use_internal_ip: bool, **kwargs):
        self.log_prefix = log_prefix
        self.node_id = node_id
        self.provider = provider
        self.auth_config = auth_config
        self.cluster_name = cluster_name
        self.process_runner = process_runner
        self.use_internal_ip = use_internal_ip

    def run(self, cmd: Optional[str] = None, timeout: int = 120, exit_on_fail: bool = False,
            port_forward: Optional[List[Tuple[int, int]]] = None, with_output: bool = False,
            environment_variables: Optional[Dict[str, object]] = None,
            run_env: str = "auto", ssh_options_override_ssh_ip: str = None,
            shutdown_after_run: bool = False) -> str:
        """Run command on the session."""
        if not cmd:
            return ""

        logger.info(f"{self.log_prefix} Running: {cmd} env: {environment_variables}")
        prefix_cmd = ["vrun", "--session-id", self.node_id, "--tty=no", "-q"]
        if isinstance(port_forward, list):
            for local_port, remote_port in port_forward:
                prefix_cmd.extend(["-L", f"{local_port}:{remote_port}"])
        elif isinstance(port_forward, tuple) and len(port_forward) == 2:
            local_port, remote_port = port_forward
            prefix_cmd.extend(["-L", f"{local_port}:{remote_port}"])
        elif not port_forward:
            pass
        else:
            raise ArgumentError("Unknown port-forward spec")
        prefix_cmd.extend(["bash", "-c", cmd])
        env = os.environ.copy()
        if environment_variables:
            env.update({str(k): json.dumps(v, separators=(",", ":")) for k, v in environment_variables.items()})

        logger.debug(f"{self.log_prefix} Executing: {prefix_cmd}")
        try:
            result = subprocess.run(
                prefix_cmd,
                check=exit_on_fail,
                capture_output=with_output,
                timeout=timeout,
                env=env,
            )
        except subprocess.TimeoutExpired as e:
            logger.error(f"{self.log_prefix} Command timed out: {e}")
            if exit_on_fail:
                raise
            return ""
        except subprocess.CalledProcessError as e:
            stderr = e.stderr.decode(errors="ignore") if e.stderr else ""
            logger.error(f"{self.log_prefix} Command failed with return code {e.returncode}: {stderr}")
            if exit_on_fail:
                raise
            return e.stdout.decode(errors="ignore") if with_output and e.stdout else ""
        else:
            if with_output:
                return result.stdout.decode(errors="ignore") if result.stdout else ""
            return ""
        # In a real implementation, this might execute the command via vrun
        # For now, we'll assume commands are handled by the node startup process
        return ""

    def run_rsync_up(self, source: str, target: str, options: Optional[Dict[str, Any]] = None) -> None:
        """Upload files using rsync. Vrun nodes may share filesystem."""
        logger.info(f"{self.log_prefix} Rsync up: {source} -> {target}")
        # Since vrun nodes may share filesystem, rsync might not be needed
        pass

    def run_rsync_down(self, source: str, target: str, options: Optional[Dict[str, Any]] = None) -> None:
        """Download files using rsync."""
        logger.info(f"{self.log_prefix} Rsync down: {source} -> {target}")
        pass

    def remote_shell_command_str(self) -> str:
        """Return remote shell command string."""
        return f"vrun -P -s ray {self.node_id}"
