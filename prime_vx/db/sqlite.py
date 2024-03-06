from pathlib import Path
from typing import Any, List

from pandas import DataFrame
from progress.bar import Bar
from sqlalchemy import Column, Date, MetaData, String, Table, create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.base import ReadOnlyColumnCollection

from prime_vx.db._classes._dbHandler import SQLiteHandler_ABC
from prime_vx.shell.fs import isDirectory, isFile, resolvePath
from prime_vx.vcs import VCS_METADATA_KEY_LIST
from typedframe import TypedDataFrame
import pandas


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
        # NOTE: tableName is hardcoded
        self.tableName: str = "vcs_metadata" 

        resolvedPath: Path = resolvePath(path=path)

        if isDirectory(path=resolvedPath):
            print(
                "Invalid path provided. Please point path to a database file, not a directory"
            )
            quit(1)

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
            Column("authorDate", Date),
            Column("committerName", String),
            Column("committerEmail", String),
            Column("committerDate", Date),
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
            print(
                "Invalid table schema. Table schema does not align with VCS_METADATA_KEYS_LIST"
            )
            quit()

    def write(self, df: DataFrame) -> None:
        dfPerRow: List[DataFrame] = [DataFrame(data=row).T for _, row in df.iterrows()]

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

    def readTable(self, tdf: type[TypedDataFrame])  ->  DataFrame:
        df: DataFrame = pandas.read_sql_table(table_name=tableName, con=self.engine,)

        return tdf(df=df).df

