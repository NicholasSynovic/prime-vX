from pathlib import Path
from typing import List

from pandas import DataFrame
from progress.bar import Bar
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from prime_vx.db._classes._dbHandler import SQLiteHandler_ABC
from prime_vx.shell.shell import isDirectory, isFile, resolvePath


class SQLiteHandler(SQLiteHandler_ABC):
    def __init__(self, path: Path) -> None:
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

    def write(self, df: DataFrame, tableName: str, engine: Engine) -> None:
        dfPerRow: List[DataFrame] = [DataFrame(data=row).T for _, row in df.iterrows()]

        with Bar(f"Writing data to {self.path}...", max=len(dfPerRow)) as bar:
            row: DataFrame
            for row in dfPerRow:
                try:
                    row.to_sql(
                        name=tableName,
                        con=engine,
                        index=False,
                        if_exists="append",
                    )
                except IntegrityError:
                    pass

                bar.next()
