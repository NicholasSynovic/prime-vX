from argparse import Namespace
from pathlib import Path
from typing import List

import pandas
from pandas import DataFrame

from prime.datamodels.cloc import CLOC_DF_DATAMODEL
from prime.datamodels.issue_tracker import IT_DF_DATAMODEL
from prime.datamodels.metrics.loc import LOC_DF_DATAMODEL
from prime.datamodels.vcs import VCS_DF_DATAMODEL
from prime.db import (
    CLOC_DB_TABLE_NAME,
    COMMIT_HASH_TO_BUS_FACTOR_BUCKET_MAP_TABLE_NAME,
    COMMIT_HASH_TO_DEVELOPER_COUNT_BUCKET_MAP_TABLE_NAME,
    COMMIT_HASH_TO_PRODUCTIVITY_BUCKET_MAP_TABLE_NAME,
    ISSUE_ID_TO_ISSUE_COUNT_BUCKET_MAP_TABLE_NAME,
    ISSUE_TRACKER_DB_TABLE_NAME,
    LOC_DB_TABLE_NAME,
    VCS_DB_TABLE_NAME,
)
from prime.db.sqlite import SQLite
from prime.exceptions import InvalidMetricSubprogram
from prime.metrics.bus_factor.main import main as bfMain
from prime.metrics.bus_factor.mapping import main as bfMapping
from prime.metrics.issue_count.main import main as icMain
from prime.metrics.issue_count.mapping import main as icMapping
from prime.metrics.issue_density.main import main as idMain
from prime.metrics.issue_spoilage.main import main as isMain
from prime.metrics.number_of_developers.main import main as nodMain
from prime.metrics.number_of_developers.mapping import main as nodMapping
from prime.metrics.productivity.main import main as prodMain
from prime.metrics.productivity.mapping import main as prodMapping
from prime.metrics.project_size.main import main as locMain


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
    locDF: DataFrame = db.read(
        tdf=LOC_DF_DATAMODEL,
        tableName=LOC_DB_TABLE_NAME,
    )

    vcsDF_locDF: DataFrame = vcsDF.merge(
        right=locDF,
        how="inner",
        on="commit_hash",
    )

    metricName: str = inputKeySplit[1]

    match metricName:
        case "size":
            df: DataFrame = locMain(df=clocDF)
            db.write(df=df, tableName=LOC_DB_TABLE_NAME, includeIndex=True)
        case "prod":
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

        case "nod":
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

        case "ic":
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

        case "is":
            dfs: dict[str, DataFrame] = isMain(df=itDF)

            # Write issue spoilage data to database
            for tableName, df in dfs.items():
                db.write(
                    df=df,
                    tableName=tableName,
                )

        case "id":
            dfs: dict[str, DataFrame] = idMain(
                vcsDF_locDF=vcsDF_locDF,
                issueDF=itDF,
            )

            # Write issue density data to database
            for tableName, df in dfs.items():
                db.write(
                    df=df,
                    tableName=tableName,
                )

        case "bf":
            bfMappingDF: DataFrame = bfMapping(df=vcsDF_locDF)
            dfs: dict[str, DataFrame] = bfMain(df=vcsDF_locDF)

            vcsDF_locDF.T.to_json(
                path_or_buf="test.json", indent=4, index=False
            )

            # Write bus factor data to database
            for tableName, df in dfs.items():
                db.write(
                    df=df,
                    tableName=tableName,
                )

            # Write bus factor mapping to database
            db.write(
                df=bfMappingDF,
                tableName=COMMIT_HASH_TO_BUS_FACTOR_BUCKET_MAP_TABLE_NAME,
                includeIndex=True,
            )

        case _:
            raise InvalidMetricSubprogram
