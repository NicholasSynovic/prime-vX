from typing import List, Literal

from numpy import datetime64
from pandas import DataFrame, DatetimeIndex, Timestamp, date_range
from progress.bar import Bar

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
) -> dict[str, List[int | datetime64]]:
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
                nextDate = Timestamp().max

            spoiledIssuesCount: int = df[
                (df["date_opened"] >= currentDate) & (df["date_closed"] < nextDate)
            ].shape[0]

            data["bucket"].append(bucket)
            data["bucket_start"].append(currentDate.to_datetime64())
            data["bucket_end"].append(nextDate.to_datetime64())
            data["spoiled_issues"].append(spoiledIssuesCount)

        return data


def main(df: DataFrame) -> dict[str, DataFrame]:
    computeIssueSpoilage(df=df, freq="D")
