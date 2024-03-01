from pathlib import Path
from typing import Any, List

from pandas import DataFrame
from progress.bar import Bar
from sqlalchemy import Column, MetaData, String, Table, create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.base import ReadOnlyColumnCollection

from prime_vx.db._classes._dbHandler import SQLiteHandler_ABC
from prime_vx.shell.shell import isDirectory, isFile, resolvePath
from prime_vx.vcs import VCS_METADATA_KEYS


class VCS_DB(SQLiteHandler_ABC):
    """
    VCS_DB

    Database handler specifically for handling version control system (VCS) metadata of repositories.
    """

    def __init__(self, path: Path, tableName: str = "vcs_metadata") -> None:
        """
        __init__

        Initalize class with specified parameters and connect to database at *path*.

        :param path: Path to SQLite3 database
        :type path: Path
        :param tableName: Name of table to store data to, defaults to "vcs_metadata"
        :type tableName: str, optional
        """
        self.tableName: str = tableName

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
            Column("authorDate", String),
            Column("committerName", String),
            Column("committerEmail", String),
            Column("committerDate", String),
            Column("refName", String),
            Column("refNameSource", String),
            Column("gpgSignature", String),
        )

        columnData: ReadOnlyColumnCollection[str, Column[Any]] = table.columns

        # TODO: Move database table schema validation to data_model module
        if [x.key for x in columnData] == VCS_METADATA_KEYS:
            metadata.create_all(bind=self.engine)
        else:
            print(
                "Invalid table schema. Table schema does not align with VCS_METADATA_KEYS"
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
