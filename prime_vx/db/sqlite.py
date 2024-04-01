from pathlib import Path

import pandas
from pandas import DataFrame
from pyfs import isDirectory, isFile, resolvePath
from sqlalchemy import (
    Column,
    DateTime,
    Engine,
    Float,
    ForeignKeyConstraint,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    create_engine,
    event,
)
from sqlalchemy.exc import IntegrityError
from typedframe import TypedDataFrame

from prime_vx.db import CLOC_DB_TABLE_NAME, LOC_DB_TABLE_NAME, VCS_DB_TABLE_NAME
from prime_vx.db._classes._dbHandler import SQLiteHandler_ABC
from prime_vx.exceptions import InvalidDBPath


class SQLite(SQLiteHandler_ABC):
    """
    SQLite

    A class for reading from and writing to SQLite3 databases.
    """

    def __init__(self, path: Path) -> None:
        """
        __init__

        Initalize the class pointing to a SQLite3 database file (.db)

        :param path: A path to a valid SQLite3 file
        :type path: Path
        :raises InvalidDBPath: If the SQLite3 file path is invalid, raises error
        """
        resolvedPath: Path = resolvePath(path=path)

        if isDirectory(path=resolvedPath):
            raise InvalidDBPath

        self.path = resolvedPath

        if isFile(path=self.path):
            self.exists = True
        else:
            self.exists = False

        self.engine = create_engine(url=f"sqlite:///{self.path}")

        @event.listens_for(Engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record) -> None:
            """
            set_sqlite_pragma

            Enable foreign key support if disabled.
            """
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    def createTables(self) -> None:
        """
        createTables

        Creates tables within the SQLite3 database to host data along with
        key types.

        This only needs to be ran once per new database to create tables.
        Running this multiple times should not harm the database, but is
        frowned upon.
        """
        metadata: MetaData = MetaData()

        vcsTable: Table = Table(
            VCS_DB_TABLE_NAME,
            metadata,
            Column("commitHash", String),
            Column("treeHash", String),
            Column("parentHashes", String),
            Column("authorName", String),
            Column("authorEmail", String),
            Column("authorDate", DateTime),
            Column("committerName", String),
            Column("committerEmail", String),
            Column("committerDate", DateTime),
            Column("refName", String),
            Column("refNameSource", String),
            Column("gpgSignature", String),
            Column("vcs", String),
            Column("path", String),
            PrimaryKeyConstraint("commitHash"),
        )

        clocTable: Table = Table(
            CLOC_DB_TABLE_NAME,
            metadata,
            Column("index", Integer),
            Column("commitHash", String),
            Column("fileCount", Integer),
            Column("lineCount", Integer),
            Column("blankLineCount", Integer),
            Column("commentLineCount", Integer),
            Column("codeLineCount", Integer),
            Column("json", String),
            PrimaryKeyConstraint("index"),
            ForeignKeyConstraint(
                columns=["commitHash"],
                refcolumns=[f"{VCS_DB_TABLE_NAME}.commitHash"],
            ),
        )

        locTable: Table = Table(
            LOC_DB_TABLE_NAME,
            metadata,
            Column("index", Integer),
            Column("commitHash", String),
            Column("loc", Integer),
            Column("kloc", Float),
            Column("delta_loc", Integer),
            Column("delta_kloc", Float),
            PrimaryKeyConstraint("index"),
            ForeignKeyConstraint(
                columns=["commitHash"],
                refcolumns=[f"{VCS_DB_TABLE_NAME}.commitHash"],
            ),
        )

        metadata.create_all(bind=self.engine, checkfirst=True)

    def write(self, df: DataFrame, tableName: str, includeIndex: bool = False) -> None:
        """
        write

        Write pandas DataFrame to specific table.

        :param df: Pandas DataFrame to write data from
        :type df: DataFrame
        :param tableName: Name of table within database to write data to
        :type tableName: str
        :param includeIndex: Include the Pandas DataFrame index as a column, defaults to False
        :type includeIndex: bool, optional
        """
        try:
            df.to_sql(
                name=tableName,
                con=self.engine,
                index=includeIndex,
                index_label="index",
                if_exists="append",
            )
        except IntegrityError:
            pass

    def read(self, tdf: type[TypedDataFrame], tableName: str) -> DataFrame:
        """
        read

        Read data from a table and ensure that it aligns with the expected output via a TypedDataFrame.

        :param tdf: A TypedDataFrame to validate the table data against
        :type tdf: type[TypedDataFrame]
        :param tableName: The name of the table to read from
        :type tableName: str
        :return: A DataFrame that's been validated against a TypedDataFrame
        :rtype: DataFrame
        """
        df: DataFrame = pandas.read_sql_table(
            table_name=tableName,
            con=self.engine,
        )

        return tdf(df=df).df
