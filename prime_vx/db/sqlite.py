from pathlib import Path

from prime_vx.db._classes._dbHandler import DBHandler_ABC
from prime_vx.shell.shell import isDirectory, isFile, resolvePath


class SQLiteHandler(DBHandler_ABC):
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


def writeToDB(df: DataFrame, dbTableName: str, dbEngine: Engine) -> None:
    dfPerRow: List[DataFrame] = [DataFrame(data=row).T for _, row in df.iterrows()]

    with Bar("Writing data to database...", max=len(dfPerRow)) as bar:
        row: DataFrame
        for row in dfPerRow:
            try:
                row.to_sql(
                    name=dbTableName, con=dbEngine, index=False, if_exists="append"
                )
            except IntegrityError:
                pass

            bar.next()
