from abc import ABCMeta, abstractmethod
from string import Template
from typing import List, Protocol

from pandas import DataFrame
from requests import Response


class ITHandler_Protocol(Protocol):
    token: str
    endpoint: Template
    ...


class ITHandler_ABC(ITHandler_Protocol, metaclass=ABCMeta):
    @abstractmethod
    def getResponses(self) -> List[Response]: ...

    @abstractmethod
    def extractIssues(self, resps: List[Response]) -> DataFrame: ...
