from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import Protocol

from pandas import DataFrame
from sqlalchemy.engine import Engine
from typedframe import TypedDataFrame


class DBHandler_Protocol(Protocol):
    path: Path
    exists: bool


class SQLiteHandler_Protocol(DBHandler_Protocol, Protocol):
    engine: Engine


class SQLiteHandler_ABC(SQLiteHandler_Protocol, metaclass=ABCMeta):
    @abstractmethod
    def createTables(self) -> None:
        ...

    @abstractmethod
    def write(self, df: DataFrame, tableName: str) -> None:
        ...

    @abstractmethod
    def read(self, tdf: type[TypedDataFrame], tableName: str) -> DataFrame:
        ...
