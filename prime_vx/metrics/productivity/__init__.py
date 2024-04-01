from typing import List, Tuple

from pandas import DataFrame, Grouper
from pandas.core.groupby.generic import DataFrameGroupBy
from progress.bar import Bar

from prime_vx.db import (
    ANNUAL_PRODUCTIVITY_DB_TABLE_NAME,
    DAILY_PRODUCTIVITY_DB_TABLE_NAME,
    MONTHLY_PRODUCTIVITY_DB_TABLE_NAME,
    SIX_MONTH_PRODUCTIVITY_DB_TABLE_NAME,
    THREE_MONTH_PRODUCTIVITY_DB_TABLE_NAME,
    TWO_MONTH_PRODUCTIVITY_DB_TABLE_NAME,
    TWO_WEEK_PRODUCTIVITY_DB_TABLE_NAME,
    WEEKLY_PRODUCTIVITY_DB_TABLE_NAME,
)

INTERVAL_PAIRS: List[Tuple[str, str]] = [
    (DAILY_PRODUCTIVITY_DB_TABLE_NAME, "D"),
    (WEEKLY_PRODUCTIVITY_DB_TABLE_NAME, "W"),
    (TWO_WEEK_PRODUCTIVITY_DB_TABLE_NAME, "2W"),
    (MONTHLY_PRODUCTIVITY_DB_TABLE_NAME, "ME"),
    (TWO_MONTH_PRODUCTIVITY_DB_TABLE_NAME, "2ME"),
    (THREE_MONTH_PRODUCTIVITY_DB_TABLE_NAME, "QE"),
    (SIX_MONTH_PRODUCTIVITY_DB_TABLE_NAME, "2QE"),
    (ANNUAL_PRODUCTIVITY_DB_TABLE_NAME, "YE"),
]


def createGroups(
    df: DataFrame,
    key: str = "committerDate",
) -> List[Tuple[DataFrameGroupBy]]:
    dfs: List[DataFrameGroupBy] = []

    with Bar("Computing groups by time interval...", max=len(INTERVAL_PAIRS)) as bar:
        pair: Tuple[str, str]
        for pair in INTERVAL_PAIRS:
            group: DataFrameGroupBy = df.groupby(
                by=Grouper(
                    key=key,
                    freq=pair[1],
                ),
            )

            dfs.append(group)
            bar.next()

    return dfs
