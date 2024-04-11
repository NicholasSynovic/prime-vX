from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import Protocol, runtime_checkable

from prime_vx.datamodels.cloc import CLOC_DF_DATAMODEL


@runtime_checkable
class CLOCTool_Protocol(Protocol):
    command: str
    path: Path
    toolName: str


class CLOCTool_ABC(CLOCTool_Protocol, metaclass=ABCMeta):
    @abstractmethod
    def compute(self, commitHash: str) -> CLOC_DF_DATAMODEL:
        ...
