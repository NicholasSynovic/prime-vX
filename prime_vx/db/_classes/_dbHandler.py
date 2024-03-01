from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import Protocol

from pandas import DataFrame
from sqlalchemy.engine import Engine


class DBHandler_Protocol(Protocol):
    path: Path
    exists: bool


class SQLiteHandler_ABC(DBHandler_Protocol, metaclass=ABCMeta):
    @abstractmethod
    def write(self, df: DataFrame, tableName: str, engine: Engine) -> None:
        ...
