from collections import namedtuple
from typing import List, Tuple

from pandas import DataFrame, Grouper
from pandas.core.groupby.generic import DataFrameGroupBy
from progress.bar import Bar

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
    defaults=[None, None, None, None, None, None, None, None],
)

INTERVAL_PAIRS: List[Tuple[str, str]] = [
    (DAILY_DEVELOPER_COUNT_DB_TABLE_NAME, "D"),
    (WEEKLY_DEVELOPER_COUNT_DB_TABLE_NAME, "W"),
    (TWO_WEEK_DEVELOPER_COUNT_DB_TABLE_NAME, "2W"),
    (MONTHLY_DEVELOPER_COUNT_DB_TABLE_NAME, "ME"),
    (TWO_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME, "2ME"),
    (THREE_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME, "QE"),
    (SIX_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME, "2QE"),
    (ANNUAL_DEVELOPER_COUNT_DB_TABLE_NAME, "YE"),
]


def createGroups(
    df: DataFrame,
    key: str = "committer_date",
) -> List[Tuple[str, DataFrameGroupBy]]:
    dfs: List[Tuple[str, DataFrameGroupBy]] = []

    with Bar("Computing groups by time interval...", max=len(INTERVAL_PAIRS)) as bar:
        pair: Tuple[str, str]
        for pair in INTERVAL_PAIRS:
            group: DataFrameGroupBy = df.groupby(
                by=Grouper(
                    key=key,
                    freq=pair[1],
                ),
            )

            dfs.append((pair[0], group))
            bar.next()

    return dfs
