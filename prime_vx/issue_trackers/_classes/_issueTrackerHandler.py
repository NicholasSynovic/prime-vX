from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import List, Protocol

from prime_vx.datamodels.vcs import VCS_DF_DATAMODEL


class ITHandler_Protocol(Protocol):
    ...


class ITHandler_ABC(ITHandler_Protocol, metaclass=ABCMeta):
    @abstractmethod
    def getJSON(self) -> dict:
        ...
