from collections import namedtuple
from datetime import datetime
from typing import List, Tuple

from pandas import DataFrame, Timestamp
from pandas.core.groupby.generic import DataFrameGroupBy
from progress.bar import Bar

from prime_vx.datamodels.metrics.issue_count import ISSUE_COUNT_DF_DATAMODEL
from prime_vx.db import (
    ANNUAL_ISSUE_COUNT_DB_TABLE_NAME,
    DAILY_ISSUE_COUNT_DB_TABLE_NAME,
    MONTHLY_ISSUE_COUNT_DB_TABLE_NAME,
    SIX_MONTH_ISSUE_COUNT_DB_TABLE_NAME,
    THREE_MONTH_ISSUE_COUNT_DB_TABLE_NAME,
    TWO_MONTH_ISSUE_COUNT_DB_TABLE_NAME,
    TWO_WEEK_ISSUE_COUNT_DB_TABLE_NAME,
    WEEKLY_ISSUE_COUNT_DB_TABLE_NAME,
)
from prime_vx.metrics import createGroups
from prime_vx.metrics.issue_count import INTERVAL_PAIRS

BUCKET_STOR = namedtuple(
    typename="BUCKET_STOR",
    field_names=[
        DAILY_ISSUE_COUNT_DB_TABLE_NAME,
        WEEKLY_ISSUE_COUNT_DB_TABLE_NAME,
        TWO_WEEK_ISSUE_COUNT_DB_TABLE_NAME,
        MONTHLY_ISSUE_COUNT_DB_TABLE_NAME,
        TWO_MONTH_ISSUE_COUNT_DB_TABLE_NAME,
        THREE_MONTH_ISSUE_COUNT_DB_TABLE_NAME,
        SIX_MONTH_ISSUE_COUNT_DB_TABLE_NAME,
        ANNUAL_ISSUE_COUNT_DB_TABLE_NAME,
    ],
)

ISSUE_ID_TO_BUCKET_MAPPING: dict[str, BUCKET_STOR] = {}


def countIssues(groups: DataFrameGroupBy, frequency: str) -> DataFrame:
    data: dict[str, List[int | float | datetime]] = {
        "bucket": [],
        "bucket_start": [],
        "bucket_end": [],
        "issue_count": [],
    }

    bucket: int = 1

    with Bar(f"Computing {frequency.replace('_', ' ')}...", max=len(groups)) as bar:
        group: DataFrame
        for _, group in groups:
            data["bucket"].append(bucket)
            data["bucket_start"].append(
                group["date_opened"].min().to_pydatetime().replace(tzinfo=None)
            )
            data["bucket_end"].append(
                group["date_opened"].max().to_pydatetime().replace(tzinfo=None)
            )
            data["issue_count"].append(group.shape[0])

            id_: str
            for id_ in group["id"]:
                setattr(
                    ISSUE_ID_TO_BUCKET_MAPPING[id_],
                    frequency,
                    bucket,
                )

            bucket += 1
            bar.next()

    return ISSUE_COUNT_DF_DATAMODEL(df=DataFrame(data=data)).df


def main(df: DataFrame) -> dict[str, DataFrame]:
    global ISSUE_ID_TO_BUCKET_MAPPING

    dfDict: dict[str, DataFrame] = {}

    issueIDs: List[str] = df["id"].to_list()
    ISSUE_ID_TO_BUCKET_MAPPING = {id_: BUCKET_STOR for id_ in issueIDs}

    groups: List[Tuple[str, DataFrameGroupBy]] = createGroups(
        df=df, intervalPairs=INTERVAL_PAIRS, key="date_opened"
    )

    group: Tuple[str, DataFrameGroupBy]
    for group in groups:
        frequency: str = group[0]

        dfDict[frequency] = countIssues(
            groups=group[1],
            frequency=frequency,
        )

    return dfDict
