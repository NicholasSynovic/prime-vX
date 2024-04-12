from argparse import Namespace
from pathlib import Path
from typing import List

from pandas import DataFrame

from prime_vx.datamodels.cloc import CLOC_DF_DATAMODEL
from prime_vx.datamodels.metrics.loc import LOC_DF_DATAMODEL
from prime_vx.datamodels.vcs import VCS_DF_DATAMODEL
from prime_vx.db import (
    CLOC_DB_TABLE_NAME,
    COMMIT_HASH_TO_DEVELOPER_COUNT_BUCKET_MAP_TABLE_NAME,
    COMMIT_HASH_TO_PRODUCTIVITY_BUCKET_MAP_TABLE_NAME,
    LOC_DB_TABLE_NAME,
    VCS_DB_TABLE_NAME,
)
from prime_vx.db.sqlite import SQLite
from prime_vx.exceptions import InvalidMetricSubprogram
from prime_vx.metrics.issue_count.main import main as icMain
from prime_vx.metrics.issue_count.mapping import main as icMapping
from prime_vx.metrics.number_of_developers.main import main as nodMain
from prime_vx.metrics.number_of_developers.mapping import main as nodMapping
from prime_vx.metrics.productivity.main import main as prodMain
from prime_vx.metrics.productivity.mapping import main as prodMapping
from prime_vx.metrics.project_size.main import main as locMain


def main(namespace: Namespace, db: SQLite) -> None:
    programInput: dict[str, List[Path]] = dict(namespace._get_kwargs())
    programKeys: List[str] = list(programInput.keys())

    inputKey: str = [key for key in programKeys if "input" in key][0]
    inputKeySplit: List[str] = inputKey.split(sep=".")

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
        case "project_size":
            df: DataFrame = locMain(df=clocDF)
            db.write(df=df, tableName=LOC_DB_TABLE_NAME, includeIndex=True)
        case "productivity":
            locDF: DataFrame = db.read(
                tdf=LOC_DF_DATAMODEL,
                tableName=LOC_DB_TABLE_NAME,
            )

            mergedDF: DataFrame = vcsDF.join(
                other=locDF.set_index("commit_hash"),
                on="commit_hash",
            )

            prodMappingDF: DataFrame = prodMapping(df=mergedDF)
            dfs: dict[str, DataFrame] = prodMain(df=mergedDF)

            # Write prod. data to database
            tableName: str
            for tableName, df in dfs.items():
                db.write(
                    df=df,
                    tableName=tableName,
                )

            # Write prod. mapping to database
            db.write(
                df=prodMappingDF,
                tableName=COMMIT_HASH_TO_PRODUCTIVITY_BUCKET_MAP_TABLE_NAME,
                includeIndex=True,
            )

        case "number_of_developers":
            nodMappingDF: DataFrame = nodMapping(df=vcsDF)
            dfs: dict[str, DataFrame] = nodMain(df=vcsDF)

            # Write nod data to database
            for tableName, df in dfs.items():
                db.write(
                    df=df,
                    tableName=tableName,
                )

            # Write nod mapping to database
            db.write(
                df=nodMappingDF,
                tableName=COMMIT_HASH_TO_DEVELOPER_COUNT_BUCKET_MAP_TABLE_NAME,
                includeIndex=True,
            )

        case "issue_count":
            nodMappingDF: DataFrame = nodMapping(df=vcsDF)
            dfs: dict[str, DataFrame] = nodMain(df=vcsDF)

            # Write nod data to database
            for tableName, df in dfs.items():
                db.write(
                    df=df,
                    tableName=tableName,
                )

            # Write nod mapping to database
            db.write(
                df=nodMappingDF,
                tableName=COMMIT_HASH_TO_DEVELOPER_COUNT_BUCKET_MAP_TABLE_NAME,
                includeIndex=True,
            )
        case _:
            raise InvalidMetricSubprogram
