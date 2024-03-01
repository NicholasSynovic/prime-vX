from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import Protocol


class DBHandler_Protocol(Protocol):
    path: Path


class DBHandler_ABC(DBHandler_Protocol, metaclass=ABCMeta):
    @abstractmethod
    def write(self) -> bool:
        ...
