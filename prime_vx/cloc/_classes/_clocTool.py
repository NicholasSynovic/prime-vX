from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import Protocol, runtime_checkable

from pandas import DataFrame


@runtime_checkable
class CLOCTool_Protocol(Protocol):
    command: str
    path: Path
    toolName: str


class CLOCTool_ABC(CLOCTool_Protocol, metaclass=ABCMeta):
    @abstractmethod
    def compute(self, commitHash: str) -> DataFrame:
        ...
