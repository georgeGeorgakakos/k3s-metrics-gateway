"""Collector registry. Importing this package registers the built-ins."""
from . import builtin  # noqa: F401
from .base import REGISTRY, Collector, build, describe, param, register

__all__ = ["REGISTRY", "Collector", "build", "describe", "param", "register"]
