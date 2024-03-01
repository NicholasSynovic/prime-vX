from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import Protocol

from pandas import DataFrame
from sqlalchemy.engine import Engine


class DBHandler_Protocol(Protocol):
    """
    DBHandler_Protocol

    Top level Protocol (a class for linters to ensure that variables andmethods are defined) for database handlers to enforce common variables that are defined in all implementations
    """

    path: Path
    exists: bool


class SQLiteHandler_Protocol(DBHandler_Protocol, Protocol):
    """
    SQLiteHandler_Protocol

    SQLite3 handler specific Protcol to enforce common variables are defined in all implementations
    """

    engine: Engine


class SQLiteHandler_ABC(SQLiteHandler_Protocol, metaclass=ABCMeta):
    """
    SQLiteHandler_ABC

    SQLite3 handler abstract base class (ABC) (i.e., interface) to enforce common methods are defined in all implementations. Inherits the SQLiteHandler_Protocol Protocol to enforce variables
    """

    @abstractmethod
    def createMetadata(self) -> None:
        """
        createMetadata

        Create table columns and metadata relevant to the specific SQLite3 handler and ensure that the SQLAlchemy engine is aware of them.
        """
        ...

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        """
        write

        Write DataFrame rows to database. Ignore rows where the primary key is already stored in the database.

        :param df: A DataFrame of information to store in the database. DataFrame column names must be the same as database column names
        :type df: DataFrame
        """
        ...
