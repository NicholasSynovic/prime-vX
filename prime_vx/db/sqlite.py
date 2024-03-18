from pathlib import Path

import pandas
from pandas import DataFrame
from sqlalchemy import (
    Column,
    DateTime,
    Engine,
    ForeignKey,
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

from prime_vx.db import LOC_DB_TABLE_NAME, CLOC_DB_TABLE_NAME, VCS_DB_TABLE_NAME
from prime_vx.db._classes._dbHandler import SQLiteHandler_ABC
from prime_vx.exceptions import InvalidDBPath
from prime_vx.shell.fs import isDirectory, isFile, resolvePath


class Generic_DB(SQLiteHandler_ABC):
    # TODO: Add doc string

    def __init__(self, path: Path) -> None:
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
        def set_sqlite_pragma(dbapi_connection, connection_record):
            # TODO: Add doc string
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    def createTables(self) -> None:
        # TODO: Add doc string
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
            Column("kloc", Integer),
            Column("delta_loc", Integer),
            Column("delta_kloc", Integer),
            PrimaryKeyConstraint("index"),
            ForeignKeyConstraint(
                columns=["commitHash"],
                refcolumns=[f"{VCS_DB_TABLE_NAME}.commitHash"],
            ),
        )

        metadata.create_all(bind=self.engine)

    def write(self, df: DataFrame, tableName: str, includeIndex: bool = False) -> None:
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
        df: DataFrame = pandas.read_sql_table(
            table_name=tableName,
            con=self.engine,
        )

        return tdf(df=df).df
