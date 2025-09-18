"""Task decomposition strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - import cycle guard
    from .main import Task, SimpleOrchestrator


class DecompositionStrategy(ABC):
    """Interface for feature decomposition strategies."""

    def __init__(self, orchestrator: "SimpleOrchestrator") -> None:
        self.orchestrator = orchestrator

    @abstractmethod
    def decompose(self, feature_id: str, feature_request: str) -> List["Task"]:
        """Return an ordered list of tasks for the feature."""


class NaiveDecomposition(DecompositionStrategy):
    """Baseline keyword-driven decomposition used as fallback."""

    def decompose(self, feature_id: str, feature_request: str) -> List["Task"]:
        return self.orchestrator._decompose_naive(feature_id, feature_request)
