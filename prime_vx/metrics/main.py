from argparse import Namespace
from pathlib import Path
from typing import List

from pandas import DataFrame

from prime_vx.datamodels.cloc import CLOC_DF_DATAMODEL
from prime_vx.datamodels.issue_tracker import IT_DF_DATAMODEL
from prime_vx.datamodels.metrics.loc import LOC_DF_DATAMODEL
from prime_vx.datamodels.vcs import VCS_DF_DATAMODEL
from prime_vx.db import (
    CLOC_DB_TABLE_NAME,
    COMMIT_HASH_TO_BUS_FACTOR_BUCKET_MAP_TABLE_NAME,
    COMMIT_HASH_TO_DEVELOPER_COUNT_BUCKET_MAP_TABLE_NAME,
    COMMIT_HASH_TO_PRODUCTIVITY_BUCKET_MAP_TABLE_NAME,
    ISSUE_ID_TO_ISSUE_COUNT_BUCKET_MAP_TABLE_NAME,
    ISSUE_TRACKER_DB_TABLE_NAME,
    LOC_DB_TABLE_NAME,
    VCS_DB_TABLE_NAME,
)
from prime_vx.db.sqlite import SQLite
from prime_vx.exceptions import InvalidMetricSubprogram
from prime_vx.metrics.bus_factor.main import main as bfMain
from prime_vx.metrics.bus_factor.mapping import main as bfMapping
from prime_vx.metrics.issue_count.main import main as icMain
from prime_vx.metrics.issue_count.mapping import main as icMapping
from prime_vx.metrics.issue_spoilage.main import main as isMain
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
    itDF: DataFrame = db.read(
        tdf=IT_DF_DATAMODEL,
        tableName=ISSUE_TRACKER_DB_TABLE_NAME,
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

            # Write productivity data to database
            tableName: str
            for tableName, df in dfs.items():
                db.write(
                    df=df,
                    tableName=tableName,
                )

            # Write productivity mapping to database
            db.write(
                df=prodMappingDF,
                tableName=COMMIT_HASH_TO_PRODUCTIVITY_BUCKET_MAP_TABLE_NAME,
                includeIndex=True,
            )

        case "number_of_developers":
            nodMappingDF: DataFrame = nodMapping(df=vcsDF)
            dfs: dict[str, DataFrame] = nodMain(df=vcsDF)

            # Write number of developers data to database
            for tableName, df in dfs.items():
                db.write(
                    df=df,
                    tableName=tableName,
                )

            # Write number of developers mapping to database
            db.write(
                df=nodMappingDF,
                tableName=COMMIT_HASH_TO_DEVELOPER_COUNT_BUCKET_MAP_TABLE_NAME,
                includeIndex=True,
            )

        case "issue_count":
            icMappingDF: DataFrame = icMapping(df=itDF)
            dfs: dict[str, DataFrame] = icMain(df=itDF)

            # Write issue count data to database
            for tableName, df in dfs.items():
                db.write(
                    df=df,
                    tableName=tableName,
                )

            # Write issue count mapping to database
            db.write(
                df=icMappingDF,
                tableName=ISSUE_ID_TO_ISSUE_COUNT_BUCKET_MAP_TABLE_NAME,
                includeIndex=True,
            )

        case "issue_spoilage":
            dfs: dict[str, DataFrame] = isMain(df=itDF)

            # Write issue spoilage data to database
            for tableName, df in dfs.items():
                db.write(
                    df=df,
                    tableName=tableName,
                )

        case "bus_factor":
            nodMappingDF: DataFrame = bfMapping(df=vcsDF)
            dfs: dict[str, DataFrame] = bfMain(df=vcsDF)

            # Write number of developers data to database
            for tableName, df in dfs.items():
                db.write(
                    df=df,
                    tableName=tableName,
                )

            # Write number of developers mapping to database
            db.write(
                df=nodMappingDF,
                tableName=COMMIT_HASH_TO_DEVELOPER_COUNT_BUCKET_MAP_TABLE_NAME,
                includeIndex=True,
            )

        case _:
            raise InvalidMetricSubprogram
