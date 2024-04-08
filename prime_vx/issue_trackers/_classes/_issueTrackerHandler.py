from abc import ABCMeta, abstractmethod
from typing import Protocol

from requests import Response


class ITHandler_Protocol(Protocol):
    ...


class ITHandler_ABC(ITHandler_Protocol, metaclass=ABCMeta):
    @abstractmethod
    def getRequest(self, paginate: bool = True) -> dict:
        ...

    @abstractmethod
    def extractIssues(self, resp: Response) -> dict:
        ...
