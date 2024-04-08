from collections import namedtuple
from datetime import datetime
from typing import List, Tuple

from pandas import DataFrame
from pandas.core.groupby.generic import DataFrameGroupBy
from progress.bar import Bar

from prime_vx.datamodels.metrics.nod import DEVELOPER_COUNT_DF_DATAMODEL
from prime_vx.db import (
    ANNUAL_DEVELOPER_COUNT_DB_TABLE_NAME,
    DAILY_DEVELOPER_COUNT_DB_TABLE_NAME,
    MONTHLY_DEVELOPER_COUNT_DB_TABLE_NAME,
    SIX_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME,
    THREE_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME,
    TWO_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME,
    TWO_WEEK_DEVELOPER_COUNT_DB_TABLE_NAME,
    WEEKLY_DEVELOPER_COUNT_DB_TABLE_NAME,
)
from prime_vx.metrics.productivity import createGroups

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
        "author_count": [],
        "author_email_count": [],
        "committer_count": [],
        "committer_email_count": [],
    }

    bucket: int = 1

    with Bar(f"Computing {frequency.replace('_', ' ')}...", max=len(groups)) as bar:
        group: DataFrame
        for _, group in groups:
            authorCount: int
            authorEmailCount: int
            committerCount: int
            committerEmailCount: int

            data["bucket"].append(bucket)
            data["bucket_start"].append(group["committer_date"].min().to_pydatetime())
            data["bucket_end"].append(group["committer_date"].max().to_pydatetime())
            data["author_count"]
            data["author_email_count"]
            data["committer_count"]
            data["committer_email_count"]

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
    """
    main

    Wrapper to compute the productivity of a project at different time intervals

    :param df: A DataFrame containing relevant information (i.e LOC and commit time)
    :type df: DataFrame
    :return: A dictionary where each key is a time interval (as a string) and each value is a DataFrame of the productivity throughout that time interval
    :rtype: dict[str, DataFrame]
    """
    global COMMIT_HASH_TO_BUCKET_MAPPING

    dfDict: dict[str, DataFrame] = {}

    hashes: List[str] = df["commit_hash"].to_list()
    COMMIT_HASH_TO_BUCKET_MAPPING = {hash_: BUCKET_STOR for hash_ in hashes}

    groups: List[Tuple[str, DataFrameGroupBy]] = createGroups(df=df)

    group: Tuple[str, DataFrameGroupBy]
    for group in groups:
        frequency: str = group[0]

        dfDict[frequency] = computeProductivity(
            groups=group[1],
            frequency=frequency,
        )

    return dfDict
