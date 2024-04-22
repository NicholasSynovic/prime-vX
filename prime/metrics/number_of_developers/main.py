from collections import namedtuple
from datetime import datetime
from typing import List, Tuple

from pandas import DataFrame
from pandas.core.groupby.generic import DataFrameGroupBy
from progress.bar import Bar

from prime.datamodels.metrics.number_of_developers import DEVELOPER_COUNT_DF_DATAMODEL
from prime.db import (
    ANNUAL_DEVELOPER_COUNT_DB_TABLE_NAME,
    DAILY_DEVELOPER_COUNT_DB_TABLE_NAME,
    MONTHLY_DEVELOPER_COUNT_DB_TABLE_NAME,
    SIX_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME,
    THREE_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME,
    TWO_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME,
    TWO_WEEK_DEVELOPER_COUNT_DB_TABLE_NAME,
    WEEKLY_DEVELOPER_COUNT_DB_TABLE_NAME,
)
from prime.metrics import createGroups
from prime.metrics.number_of_developers import INTERVAL_PAIRS

BUCKET_STOR = namedtuple(
    typename="BUCKET_STOR",
    field_names=[
        DAILY_DEVELOPER_COUNT_DB_TABLE_NAME,
        WEEKLY_DEVELOPER_COUNT_DB_TABLE_NAME,
        TWO_WEEK_DEVELOPER_COUNT_DB_TABLE_NAME,
        MONTHLY_DEVELOPER_COUNT_DB_TABLE_NAME,
        TWO_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME,
        THREE_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME,
        SIX_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME,
        ANNUAL_DEVELOPER_COUNT_DB_TABLE_NAME,
    ],
)

COMMIT_HASH_TO_BUCKET_MAPPING: dict[str, BUCKET_STOR] = {}


def countDevelopers(groups: DataFrameGroupBy, frequency: str) -> DataFrame:
    data: dict[str, List[int | float | datetime]] = {
        "bucket": [],
        "bucket_start": [],
        "bucket_end": [],
        "developer_count": [],
    }

    bucket: int = 1

    with Bar(f"Computing {frequency.replace('_', ' ')}...", max=len(groups)) as bar:
        group: DataFrame
        for _, group in groups:
            data["bucket"].append(bucket)
            data["bucket_start"].append(
                group["committer_date"].min().to_pydatetime().replace(tzinfo=None)
            )
            data["bucket_end"].append(
                group["committer_date"].max().to_pydatetime().replace(tzinfo=None)
            )
            data["developer_count"].append(group["committer_email"].unique().size)

            hash_: str
            for hash_ in group["commit_hash"]:
                setattr(
                    COMMIT_HASH_TO_BUCKET_MAPPING[hash_],
                    frequency,
                    bucket,
                )

            bucket += 1
            bar.next()

    return DEVELOPER_COUNT_DF_DATAMODEL(df=DataFrame(data=data)).df


def main(df: DataFrame) -> dict[str, DataFrame]:
    global COMMIT_HASH_TO_BUCKET_MAPPING

    dfDict: dict[str, DataFrame] = {}

    hashes: List[str] = df["commit_hash"].to_list()
    COMMIT_HASH_TO_BUCKET_MAPPING = {hash_: BUCKET_STOR for hash_ in hashes}

    groups: List[Tuple[str, DataFrameGroupBy]] = createGroups(
        df=df,
        intervalPairs=INTERVAL_PAIRS,
    )

    group: Tuple[str, DataFrameGroupBy]
    for group in groups:
        frequency: str = group[0]

        dfDict[frequency] = countDevelopers(
            groups=group[1],
            frequency=frequency,
        )

    return dfDict
