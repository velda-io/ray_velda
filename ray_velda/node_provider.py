import subprocess
import logging
import random
import string
import json
from threading import RLock
from typing import Any, Dict, List, Optional

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    TAG_RAY_NODE_KIND,
)

from .command_runner import VeldaCommandRunner

logger = logging.getLogger(__name__)

# Node states
NODE_STATE_RUNNING = "running"
NODE_STATE_PENDING = "pending"
NODE_STATE_TERMINATED = "terminated"


class VeldaNodeProvider(NodeProvider):
    """
    Node provider that uses vrun to start Ray worker nodes and velda to stop them.

    To be compatible with Ray autoscaler, the node creation just sleep indefinitely with
    the session ID as the node ID.
    """

    def __init__(self, provider_config: Dict[str, Any], cluster_name: str):
        self.provider_config = provider_config
        self.cluster_name = 'ray-' + cluster_name

        # Configuration
        self.pool = provider_config.get("pool", "shell")
        self.node_prefix = provider_config.get("node_prefix", "ray-worker")

        # Node tracking
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.node_tags_cache: Dict[str, Dict[str, str]] = {}
        self.lock = RLock()

        # IP caches for node lookups
        self._internal_ip_cache: Dict[str, str] = {}

    def _generate_node_id(self) -> str:
        """Generate a random node ID."""
        suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        return f"{self.node_prefix}-{suffix}"

    def _execute_command(self, command: List[str], check: bool = True) -> subprocess.CompletedProcess:
        """Execute a command and return the result."""
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=check)
            return result
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {e}")
            logger.error(f"Stdout: {e.stdout}")
            logger.error(f"Stderr: {e.stderr}")
            raise

    def prepare_for_head_node(self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare configuration for head node."""
        return cluster_config

    def create_node(self, node_config: Dict[str, Any], tags: Dict[str, str], count: int) -> Optional[Dict[str, Any]]:
        """
        Create new nodes using vrun command.

        Command format: vbatch -P [pool] -s [cluster-name] --force-new-session sleep inf
        """
        result = dict()
        merged_tags = ','.join([f"{k}={v}" for k, v in tags.items()])
        for i in range(count):
            cmd = [
                "vrun",
                "-P", self.pool,
                '--new-session',
                '--keep-alive',
                '--tty=no',
                '--tags=' + merged_tags,
                '-q',
            ]
            if tags.get(TAG_RAY_NODE_KIND, None) == "head":
                cmd.extend(["-s", self.cluster_name])
            cmd.extend([
                "sh", "-c",
                "hostname; sleep inf&"])
            cmd = subprocess.run(cmd, stdout=subprocess.PIPE, text=True, check=True)
            session_id = cmd.stdout.strip()
            with self.lock:
                result[session_id] = dict(session_id=session_id)
        return result

    def terminate_node(self, node_id: str) -> None:
        """
        Terminate a node using velda kill command.

        Command format: velda task kill [node_id]
        """

        cmd = ["velda", "kill", '--session-id', node_id]
        subprocess.run(cmd, check=True)
        with self.lock:
            self.nodes.pop(node_id, None)
        return

    def terminate_nodes(self, node_ids: List[str]) -> Optional[Dict[str, Any]]:
        """Terminate multiple nodes."""
        for node_id in node_ids:
            self.terminate_node(node_id)
        return None

    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        """Return list of non-terminated node IDs matching the tag filters."""
        matching_nodes = []

        nodes = subprocess.run([
            "velda",
            "ls",
            #"-s", self.cluster_name,
            "-o", "json"],
            capture_output=True, check=True)
        node_list = json.loads(nodes.stdout)

        nodes_map = {}
        ip_cache = {}
        for node in node_list['sessions']:
            # Ray autoscaler references this tag without checking its value
            if TAG_RAY_NODE_KIND not in node.get("tags", {}):
                continue
            nodes_map[node["session_id"]] = node
            internal_ip = node.get("internal_ip_address")
            if internal_ip:
                ip_cache[internal_ip] = node["session_id"]

        with self.lock:
            self.nodes = nodes_map
            self._internal_ip_cache = ip_cache
            for node_id, node_info in self.nodes.items():
                tags = node_info.get("tags", {})
                if all(tags.get(k) == v for k, v in tag_filters.items()):
                    matching_nodes.append(node_id)

        return matching_nodes

    def is_running(self, node_id: str) -> bool:
        """Check if a node is running."""
        with self.lock:
            return node_id in self.nodes

    def is_terminated(self, node_id: str) -> bool:
        """Check if a node is terminated."""
        with self.lock:
            return node_id not in self.nodes

    def node_tags(self, node_id: str) -> Dict[str, str]:
        """Get the tags for a node."""
        with self.lock:
            if node_id in self.nodes:
                return self.nodes[node_id].get("tags", {})
        return None

    def external_ip(self, node_id: str) -> Optional[str]:
        """Get external IP of a node."""
        # For vrun nodes, external IP might not be applicable
        return None

    def internal_ip(self, node_id: str) -> Optional[str]:
        """Get internal IP of a node."""
        with self.lock:
            if node_id in self.nodes:
                return self.nodes[node_id].get("internal_ip_address")
        return None

    def get_node_id(self, ip_address: str, use_internal_ip: bool = True) -> Optional[str]:
        """Get node ID from IP address."""
        cache = self._internal_ip_cache
        return cache.get(ip_address)

    def set_node_tags(self, node_id: str, tags: Dict[str, str]) -> None:
        """Set tags for a node."""
        merged_tags = ','.join([f"{k}={v}" for k, v in tags.items()])
        cmd = [
            "velda", "set-tag",
            "--session-id", node_id,
            "--tags=" + merged_tags
        ]
        subprocess.run(cmd, check=True)

    def get_command_runner(self, log_prefix: str, node_id: str, auth_config: Dict[str, Any],
                          cluster_name: str, process_runner, use_internal_ip: bool,
                          docker_config: Optional[Dict[str, Any]] = None) -> VeldaCommandRunner:
        """Get command runner for a node."""
        return VeldaCommandRunner(
            log_prefix=log_prefix,
            node_id=node_id,
            provider=self,
            auth_config=auth_config,
            cluster_name=cluster_name,
            process_runner=process_runner,
            use_internal_ip=use_internal_ip
        )
