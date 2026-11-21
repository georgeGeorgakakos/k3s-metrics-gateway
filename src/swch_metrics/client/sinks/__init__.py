"""Sink registry. Importing this package registers the built-ins."""
from . import builtin, optimusdb_trust  # noqa: F401
from .base import SINKS, Sink, build_sink, describe_sinks, register_sink

__all__ = ["SINKS", "Sink", "build_sink", "describe_sinks", "register_sink"]
