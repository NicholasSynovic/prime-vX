from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import Any, List, Protocol

from pandas import DataFrame


class VCSHandler_Protocol(Protocol):
    path: Path


class VCSHandler_ABC(VCSHandler_Protocol, metaclass=ABCMeta):
    @abstractmethod
    def isRepository(self) -> bool:
        ...

    @abstractmethod
    def getCommitHashes(self) -> List[str]:
        ...

    @abstractmethod
    def checkoutCommit(self, commitHash: str) -> bool:
        ...

    @abstractmethod
    def getCommitMetadata(self, commitHash: str) -> DataFrame:
        ...
