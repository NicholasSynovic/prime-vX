from argparse import Namespace
from pathlib import Path
from typing import List

from pandas import DataFrame
from pyfs import isFile, resolvePath

from prime_vx.db.sqlite import SQLite
from prime_vx.exceptions import InvalidDBPath, InvalidMetricSubprogram
from prime_vx.metrics.loc.main import main as locMain
from prime_vx.metrics.productivity.main import main as prodMain
from prime_vx.metrics.productivity.mapping import main as prodMapping


def main(namespace: Namespace) -> None:
    # TODO: Add docstring

    programInput: dict[str, List[Path]] = dict(namespace._get_kwargs())
    programKeys: List[str] = list(programInput.keys())

    inputKey: str = [key for key in programKeys if "input" in key][0]
    inputKeySplit: List[str] = inputKey.split(sep=".")

    dbPath: Path = programInput[inputKey][0]
    resolvedDBPath: Path = resolvePath(path=dbPath)

    if isFile(path=resolvedDBPath):
        pass
    else:
        raise InvalidDBPath

    db: SQLite = SQLite(path=resolvedDBPath)

    vcsDF: DataFrame = db.read(
        tdf=VCS_DF_DATAMODEL,
        tableName=VCS_DB_TABLE_NAME,
    )
    clocDF: DataFrame = db.read(
        tdf=CLOC_DF_DATAMODEL,
        tableName=CLOC_DB_TABLE_NAME,
    )

    metricName: str = inputKeySplit[1]

    match metricName:
        case "loc":
            df: DataFrame = locMain(df=clocDF)
            db.write(df=df, tableName=LOC_DB_TABLE_NAME, includeIndex=True)
        case "productivity":
            locDF: DataFrame = db.read(
                tdf=LOC_DF_DATAMODEL,
                tableName=LOC_DB_TABLE_NAME,
            )

            mergedDF: DataFrame = vcsDF.join(
                other=locDF.set_index("commitHash"),
                on="commitHash",
            )

            prodMappingDF: DataFrame = prodMapping(df=mergedDF)
            db.write(
                df=prodMappingDF,
                tableName=COMMIT_HASH_TO_PRODUCTIVITY_BUCKET_MAP_TABLE_NAME,
                includeIndex=True,
            )

            dfs: dict[str, DataFrame] = prodMain(df=mergedDF)

            key: str
            for tableName, df in dfs.items():
                db.write(
                    df=df,
                    tableName=tableName,
                )

        case _:
            raise InvalidMetricSubprogram
