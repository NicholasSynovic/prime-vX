from pathlib import Path
from typing import Any, List

import pandas
from pandas import DataFrame
from progress.bar import Bar
from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.base import ReadOnlyColumnCollection
from typedframe import TypedDataFrame

from prime_vx.cloc import CLOC_KEY_LIST
from prime_vx.db import CLOC_DB_TABLE_NAME, VCS_DB_TABLE_NAME
from prime_vx.db._classes._dbHandler import SQLiteHandler_ABC
from prime_vx.exceptions import InvalidDBPath, InvalidVCSTableSchema
from prime_vx.shell.fs import isDirectory, isFile, resolvePath
from prime_vx.vcs import VCS_METADATA_KEY_LIST


class VCS_DB(SQLiteHandler_ABC):
    """
    VCS_DB

    Database handler specifically for handling version control system (VCS) metadata of repositories.
    """

    def __init__(self, path: Path) -> None:
        """
        __init__

        Initalize class with specified parameters and connect to database at *path*.

        Data is written to a table called 'vcs_metadata'.

        :param path: Path to SQLite3 database
        :type path: Path
        """
        self.tableName = VCS_DB_TABLE_NAME

        resolvedPath: Path = resolvePath(path=path)

        if isDirectory(path=resolvedPath):
            raise InvalidDBPath

        self.path = resolvedPath

        if isFile(path=self.path):
            self.exists = True
        else:
            self.exists = False

        self.engine = create_engine(url=f"sqlite:///{self.path}")

    def createMetadata(self) -> None:
        metadata: MetaData = MetaData()

        table: Table = Table(
            self.tableName,
            metadata,
            Column("commitHash", String, primary_key=True),
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
        )

        columnData: ReadOnlyColumnCollection[str, Column[Any]] = table.columns

        # TODO: Move database table schema validation to data_model module
        if [x.key for x in columnData] == VCS_METADATA_KEY_LIST:
            metadata.create_all(bind=self.engine)
        else:
            raise InvalidVCSTableSchema

    def write(self, df: DataFrame) -> None:
        if self.exists == False:
            try:
                df.to_sql(
                    name=self.tableName,
                    con=self.engine,
                    index=False,
                    if_exists="replace",
                )
            except IntegrityError:
                pass
        else:
            dfPerRow: List[DataFrame] = [
                DataFrame(data=row).T for _, row in df.iterrows()
            ]

            with Bar(f"Writing data to {self.path}...", max=len(dfPerRow)) as bar:
                row: DataFrame
                for row in dfPerRow:
                    try:
                        row.to_sql(
                            name=self.tableName,
                            con=self.engine,
                            index=False,
                            if_exists="append",
                        )
                    except IntegrityError:
                        pass

                    bar.next()

    def readTable(self, tdf: type[TypedDataFrame]) -> DataFrame:
        df: DataFrame = pandas.read_sql_table(
            table_name=self.tableName,
            con=self.engine,
        )

        return tdf(df=df).df


class CLOC_DB(SQLiteHandler_ABC):
    """
    CLOC_DB

    Database handler specifically for handling CLOC-like metrics of repositories.
    """

    def __init__(self, path: Path) -> None:
        """
        __init__

        Initalize class with specified parameters and connect to database at *path*.

        Data is written to a table called 'cloc'.

        :param path: Path to SQLite3 database
        :type path: Path
        """
        self.tableName = CLOC_DB_TABLE_NAME

        resolvedPath: Path = resolvePath(path=path)

        if isDirectory(path=resolvedPath):
            raise InvalidDBPath

        self.path = resolvedPath

        if isFile(path=self.path):
            self.exists = True
        else:
            self.exists = False

        self.engine = create_engine(url=f"sqlite:///{self.path}")

    def createMetadata(self) -> None:
        metadata: MetaData = MetaData()

        table: Table = Table(
            self.tableName,
            metadata,
            Column("index", Integer, primary_key=True),
            Column("commitHash", String, ForeignKey(f"{VCS_DB_TABLE_NAME}.commitHash")),
            Column("fileCount", Integer),
            Column("lineCount", Integer),
            Column("blankLineCount", Integer),
            Column("commentLineCount", Integer),
            Column("codeLineCount", Integer),
            Column("json", String),
        )

        columnData: ReadOnlyColumnCollection[str, Column[Any]] = table.columns

        # TODO: Move database table schema validation to data_model module
        if [x.key for x in columnData] == CLOC_KEY_LIST:
            metadata.create_all(bind=self.engine)
        else:
            raise InvalidVCSTableSchema

    def write(self, df: DataFrame) -> None:
        try:
            df.to_sql(
                name=self.tableName,
                con=self.engine,
                index=True,
                index_label="index",
                if_exists="replace",
            )
        except IntegrityError:
            pass

    def readTable(self, tdf: type[TypedDataFrame]) -> DataFrame:
        df: DataFrame = pandas.read_sql_table(
            table_name=self.tableName,
            con=self.engine,
        )

        return tdf(df=df).df
