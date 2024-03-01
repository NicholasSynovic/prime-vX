from pathlib import Path
from typing import List

from pandas import DataFrame
from progress.bar import Bar
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import IntegrityError

from prime_vx.db._classes._dbHandler import SQLiteHandler_ABC
from prime_vx.db._schemas._sqlite import vcsMetadataSchema
from prime_vx.shell.shell import isDirectory, isFile, resolvePath


class VCS_DB(SQLiteHandler_ABC):
    def __init__(self, path: Path, tableName: str = "vcs_metadata") -> None:
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

    def createVCSMetadata(self) -> str:
        return vcsMetadataSchema(engine=self.engine, tableName=self.tableName)

    def write(self, df: DataFrame, engine: Engine) -> None:
        dfPerRow: List[DataFrame] = [DataFrame(data=row).T for _, row in df.iterrows()]

        with Bar(f"Writing data to {self.path}...", max=len(dfPerRow)) as bar:
            row: DataFrame
            for row in dfPerRow:
                try:
                    row.to_sql(
                        name=self.tableName,
                        con=engine,
                        index=False,
                        if_exists="append",
                    )
                except IntegrityError:
                    pass

                bar.next()
