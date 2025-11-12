"""ray_velda

Ray auto-scaler for Velda managed nodes.

Exports:
 - VeldaNodeProvider
 - VeldaCommandRunner
"""

from .node_provider import VeldaNodeProvider  # noqa: F401
from .command_runner import VeldaCommandRunner  # noqa: F401

__all__ = ["VeldaNodeProvider", "VeldaCommandRunner"]

__version__ = "0.1.0"