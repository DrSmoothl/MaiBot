"""Focus-mode helpers for Maisaka."""

from .manager import FocusTargetResolution, focus_mode_manager
from .runtime_mixin import MaisakaFocusRuntimeMixin

__all__ = [
    "FocusTargetResolution",
    "MaisakaFocusRuntimeMixin",
    "focus_mode_manager",
]
