import importlib
import inspect

from .base import PositionView, Signal, Strategy

__all__ = ["Strategy", "Signal", "PositionView", "load_strategy"]


def load_strategy(name: str, params: dict | None = None) -> Strategy:
    """Import strategies/<name>.py and instantiate the Strategy subclass defined in it."""
    module = importlib.import_module(f"strategies.{name}")
    for _, obj in inspect.getmembers(module, inspect.isclass):
        if issubclass(obj, Strategy) and obj is not Strategy and obj.__module__ == module.__name__:
            return obj(params)
    raise ValueError(f"No Strategy subclass found in strategies/{name}.py")
