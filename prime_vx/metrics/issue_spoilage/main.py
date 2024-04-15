from typing import List, Literal

from numpy import datetime64
from pandas import DataFrame, DatetimeIndex, Timestamp, date_range
from progress.bar import Bar

from prime_vx.datamodels.metrics.issue_spoilage import ISSUE_SPOILAGE_DF_DATAMODEL
from prime_vx.db import (
    ANNUAL_ISSUE_SPOILAGE_DB_TABLE_NAME,
    DAILY_ISSUE_SPOILAGE_DB_TABLE_NAME,
    MONTHLY_ISSUE_SPOILAGE_DB_TABLE_NAME,
    SIX_MONTH_ISSUE_SPOILAGE_DB_TABLE_NAME,
    THREE_MONTH_ISSUE_SPOILAGE_DB_TABLE_NAME,
    TWO_MONTH_ISSUE_SPOILAGE_DB_TABLE_NAME,
    TWO_WEEK_ISSUE_SPOILAGE_DB_TABLE_NAME,
    WEEKLY_ISSUE_SPOILAGE_DB_TABLE_NAME,
)

TIME_FORMAT: str = "%Y-%m-%d"


def computeIssueSpoilage(
    df: DataFrame,
    freq: Literal[
        "D",
        "W",
        "2W",
        "ME",
        "2ME",
        "QE",
        "2QE",
        "YE",
    ],
) -> DataFrame:
    data: dict[str, List[int | datetime64]] = {
        "bucket": [],
        "bucket_start": [],
        "bucket_end": [],
        "spoiled_issues": [],
    }

    bucket: int = 1

    minimumDate: Timestamp = df["date_opened"].min()
    maximumDate: Timestamp = df["date_closed"].max()

    minimumDateFormatted: Timestamp = Timestamp(
        ts_input=minimumDate.strftime(format=TIME_FORMAT),
    )
    maximumDateFormatted: Timestamp = Timestamp(
        ts_input=maximumDate.strftime(format=TIME_FORMAT),
    )

    dateRange: DatetimeIndex = date_range(
        start=minimumDateFormatted,
        end=maximumDateFormatted,
        freq=freq,
    )

    maxIterations: int = len(dateRange)
    with Bar(
        f"Counting number of spoiled issues (frequency: {freq})...", max=maxIterations
    ) as bar:
        idx: int
        for idx in range(maxIterations):
            currentDate: Timestamp = dateRange[idx]

            nextDate: Timestamp
            try:
                nextDate = dateRange[idx + 1]
            except IndexError:
                nextDate = Timestamp.max

            spoiledIssuesCount: int = df[
                (df["date_opened"] < df["date_closed"])
                & (df["date_closed"] > currentDate)
                & (df["date_closed"] <= nextDate)
            ].shape[0]

            data["bucket"].append(bucket)
            data["bucket_start"].append(currentDate.to_datetime64())
            data["bucket_end"].append(nextDate.to_datetime64())
            data["spoiled_issues"].append(spoiledIssuesCount)

            bucket += 1

            bar.next()

    return ISSUE_SPOILAGE_DF_DATAMODEL(df=DataFrame(data=data)).df


def main(df: DataFrame) -> dict[str, DataFrame]:
    return {
        ANNUAL_ISSUE_SPOILAGE_DB_TABLE_NAME: computeIssueSpoilage(
            df=df,
            freq="YE",
        ),
        DAILY_ISSUE_SPOILAGE_DB_TABLE_NAME: computeIssueSpoilage(
            df=df,
            freq="D",
        ),
        MONTHLY_ISSUE_SPOILAGE_DB_TABLE_NAME: computeIssueSpoilage(
            df=df,
            freq="ME",
        ),
        SIX_MONTH_ISSUE_SPOILAGE_DB_TABLE_NAME: computeIssueSpoilage(
            df=df,
            freq="2QE",
        ),
        THREE_MONTH_ISSUE_SPOILAGE_DB_TABLE_NAME: computeIssueSpoilage(
            df=df,
            freq="QE",
        ),
        TWO_MONTH_ISSUE_SPOILAGE_DB_TABLE_NAME: computeIssueSpoilage(
            df=df,
            freq="2ME",
        ),
        TWO_WEEK_ISSUE_SPOILAGE_DB_TABLE_NAME: computeIssueSpoilage(
            df=df,
            freq="2W",
        ),
        WEEKLY_ISSUE_SPOILAGE_DB_TABLE_NAME: computeIssueSpoilage(
            df=df,
            freq="W",
        ),
    }
