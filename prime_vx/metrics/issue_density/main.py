from functools import partial
from typing import List, Tuple

from intervaltree import IntervalTree
from numpy import datetime64
from pandas import DataFrame, Timedelta, Timestamp
from progress.bar import Bar

from prime_vx.datamodels.metrics.issue_density import ISSUE_DENSITY_DF_DATAMODEL
from prime_vx.db import (
    ANNUAL_ISSUE_DENSITY_DB_TABLE_NAME,
    DAILY_ISSUE_DENSITY_DB_TABLE_NAME,
    MONTHLY_ISSUE_DENSITY_DB_TABLE_NAME,
    SIX_MONTH_ISSUE_DENSITY_DB_TABLE_NAME,
    THREE_MONTH_ISSUE_DENSITY_DB_TABLE_NAME,
    TWO_MONTH_ISSUE_DENSITY_DB_TABLE_NAME,
    TWO_WEEK_ISSUE_DENSITY_DB_TABLE_NAME,
    WEEKLY_ISSUE_DENSITY_DB_TABLE_NAME,
)


def identifyDayZero_N(df: DataFrame) -> Tuple[Timestamp, Timestamp]:
    return (
        df["committer_date"].min().replace(tzinfo=None),
        Timestamp.now().replace(tzinfo=None),
    )


def buildIssueIntervalTree(df: DataFrame, dayZero: Timestamp) -> IntervalTree:
    it: IntervalTree = IntervalTree()

    start: List[Timedelta] = (df["date_opened"] - dayZero).to_list()
    end: List[Timedelta] = (df["date_closed"] - dayZero).to_list()

    with Bar("Creating interval tree...", max=df.shape[0]) as bar:
        timedelta: Tuple[Timedelta]
        for timedelta in list(zip(start, end)):
            it.addi(
                begin=timedelta[0].days,
                end=timedelta[1].days + 1,
                data=1,
            )
            bar.next()

    return it


def computeIssueDensity(
    it: IntervalTree,
    dayZero: Timestamp,
    locDF: DataFrame,
    step: int = 1,
    interval: str = "daily",
) -> DataFrame:
    data: dict[str, List[int | datetime64]] = {
        "bucket": [],
        "bucket_start": [],
        "bucket_end": [],
        "issue_density": [],
    }
    bucket: int = 1

    itSize: int = len(it)

    with Bar(f"Computing {interval} issue density...", max=itSize // step) as bar:
        for i in range(0, itSize, step):
            data["bucket"].append(bucket)

            bucketStart: datetime64 = (
                (dayZero + Timedelta(days=i))
                .to_period(freq="D")
                .to_timestamp(how="begin")
                .to_datetime64()
            )

            numberOfIssues: int
            try:
                numberOfIssues = len(it[i : i + step])
                bucketEnd: datetime64 = (
                    (dayZero + Timedelta(days=i + step - 1))
                    .to_period(freq="D")
                    .to_timestamp(how="end")
                    .to_datetime64()
                )
            except IndexError:
                numberOfIssues = len(it[i:-1])
                bucketEnd: datetime64 = (
                    (dayZero + Timedelta(days=itSize - 1))
                    .to_period(freq="D")
                    .to_timestamp(how="end")
                    .to_datetime64()
                )

            projectSize: float = locDF[
                locDF["committer_date"]
                >= Timestamp(bucketStart) & locDF["committer_date"]
                <= Timestamp(bucketEnd)
            ]["kloc"][-1]

            density: float = numberOfIssues / projectSize

            data["issue_density"].append(density)
            data["bucket_start"].append(bucketStart)
            data["bucket_end"].append(bucketEnd)

            bucket += 1
            bar.next()

    return DataFrame(data=data)
    return ISSUE_DENSITY_DF_DATAMODEL(df=DataFrame(data=data)).df


def main(issueDF: DataFrame, vcsDF_locDF: DataFrame) -> dict[str, DataFrame]:
    dayZero: Timestamp
    dayN: Timestamp
    dayZero, dayN = identifyDayZero_N(df=vcsDF_locDF)  # Aligns dates to vcsDF

    issueDF["date_opened"] = issueDF["date_opened"].fillna(value=dayZero)
    issueDF["date_closed"] = issueDF["date_closed"].fillna(value=dayN)

    issueIT: IntervalTree = buildIssueIntervalTree(df=issueDF, dayZero=dayZero)

    partialCID = partial(
        computeIssueDensity,
        it=issueIT,
        dayZero=dayZero,
        locDF=vcsDF_locDF,
    )

    annualDF: DataFrame = partialCID(step=365, interval="annual")
    dailyDF: DataFrame = partialCID(step=1, interval="daily")
    monthlyDF: DataFrame = partialCID(step=30, interval="monthly")
    sixMonthDF: DataFrame = partialCID(step=180, interval="six month")
    threeMonthDF: DataFrame = partialCID(step=90, interval="three month")
    twoMonthDF: DataFrame = partialCID(step=60, interval="two month")
    twoWeekDF: DataFrame = partialCID(step=14, interval="two week")
    weeklyDF: DataFrame = partialCID(step=7, interval="weekly")

    print(annualDF)

    return {
        ANNUAL_ISSUE_DENSITY_DB_TABLE_NAME: annualDF,
        DAILY_ISSUE_DENSITY_DB_TABLE_NAME: dailyDF,
        MONTHLY_ISSUE_DENSITY_DB_TABLE_NAME: monthlyDF,
        SIX_MONTH_ISSUE_DENSITY_DB_TABLE_NAME: sixMonthDF,
        THREE_MONTH_ISSUE_DENSITY_DB_TABLE_NAME: threeMonthDF,
        TWO_MONTH_ISSUE_DENSITY_DB_TABLE_NAME: twoMonthDF,
        TWO_WEEK_ISSUE_DENSITY_DB_TABLE_NAME: twoWeekDF,
        WEEKLY_ISSUE_DENSITY_DB_TABLE_NAME: weeklyDF,
    }
