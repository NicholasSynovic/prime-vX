from typing import List, Tuple

from intervaltree import IntervalTree
from numpy import datetime64
from pandas import DataFrame, Timedelta, Timestamp
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


def identifyDayZero_N(df: DataFrame) -> Tuple[Timestamp, Timestamp]:
    return (
        df["date_opened"].min().replace(tzinfo=None),
        Timestamp.now().replace(tzinfo=None),
    )


def buildIntervalTree(df: DataFrame, dayZero: Timestamp) -> IntervalTree:
    it: IntervalTree = IntervalTree()

    start: List[Timedelta] = (df["date_opened"] - dayZero).to_list()
    end: List[Timedelta] = (df["date_closed"] - dayZero).to_list()

    with Bar("Creating interval tree...", max=df.shape[0]) as bar:
        timedelta: Timedelta
        for timedelta in list(zip(start, end)):
            it.addi(
                begin=timedelta[0].days,
                end=timedelta[1].days + 1,
                data=1,
            )
            bar.next()

    return it


def computeIssueSpoilage(
    it: IntervalTree, dayZero: Timestamp, step: int = 1
) -> DataFrame:
    data: dict[str, List[int | datetime64]] = {
        "bucket": [],
        "bucket_start": [],
        "bucket_end": [],
        "spoiled_issues": [],
    }
    bucket: int = 1

    itSize: int = len(it)

    with Bar("Computing issue spoilage...", max=itSize // step) as bar:
        for i in range(0, itSize, step):
            data["bucket"].append(bucket)

            bucketStart: datetime64 = (
                (dayZero + Timedelta(days=i))
                .to_period(freq="D")
                .to_timestamp(how="begin")
                .to_datetime64()
            )

            try:
                data["spoiled_issues"].append(len(it[i : i + step]))
                bucketEnd: datetime64 = (
                    (dayZero + Timedelta(days=i + step - 1))
                    .to_period(freq="D")
                    .to_timestamp(how="end")
                    .to_datetime64()
                )
            except IndexError:
                data["spoiled_issues"].append(len(it[i:-1]))
                bucketEnd: datetime64 = (
                    (dayZero + Timedelta(days=itSize - 1))
                    .to_period(freq="D")
                    .to_timestamp(how="end")
                    .to_datetime64()
                )

            data["bucket_start"].append(bucketStart)
            data["bucket_end"].append(bucketEnd)

            bucket += 1
            bar.next()

    return ISSUE_SPOILAGE_DF_DATAMODEL(df=DataFrame(data=data)).df


def main(df: DataFrame) -> dict[str, DataFrame]:
    dayZero: Timestamp
    dayN: Timestamp
    dayZero, dayN = identifyDayZero_N(df=df)

    df["date_opened"] = df["date_opened"].fillna(value=dayZero)
    df["date_closed"] = df["date_closed"].fillna(value=dayN)

    timeline: List[int] = list(range((dayN - dayZero).days))

    it: IntervalTree = buildIntervalTree(df=df, dayZero=dayZero)

    df: DataFrame = computeIssueSpoilage(
        it=it,
        dayZero=dayZero,
        step=7,
    )

    print(df)
