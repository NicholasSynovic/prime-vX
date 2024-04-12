from typing import List

import pandas
from intervaltree import IntervalTree
from pandas import DataFrame, Series, Timestamp
from progress.bar import Bar

TIME_FORMAT_STR: str = "%Y-%m-%d"


def createDateToIntervalMapping(dates: Series) -> dict[Timestamp, int]:
    sortedDates: List[Timestamp] = [
        Timestamp(date)
        for date in dates.dt.strftime(date_format=TIME_FORMAT_STR).unique()
    ]

    data: dict[Timestamp, int] = {date: sortedDates.index(date) for date in sortedDates}

    return data


def createIntervalTree(dates: DataFrame, map_: dict[Timestamp, int]) -> IntervalTree:
    it: IntervalTree = IntervalTree()

    maximumDate: Timestamp = list(map_.keys())[-1]

    with Bar("Creating interval tree...", max=dates.shape[0]) as bar:
        openDate: Timestamp
        closeDate: Timestamp
        for openDate, closeDate in dates.itertuples(index=False):
            openDay: Timestamp = Timestamp(
                ts_input=openDate.strftime(format=TIME_FORMAT_STR)
            )

            closeDay: Timestamp
            try:
                closeDay = Timestamp(
                    ts_input=closeDate.strftime(format=TIME_FORMAT_STR)
                )
            except ValueError:
                closeDay = maximumDate

            startInterval: int = map_[openDay]
            endInterval: int = map_[closeDay]

            it.addi(begin=startInterval, end=endInterval + 1, data=True)

            bar.next()

    return it


def main(df: DataFrame) -> dict[str, DataFrame]:
    openDates: Series = df["date_opened"]
    closedDates: Series = df["date_closed"]

    datesDF: DataFrame = df[["date_opened", "date_closed"]]
    datesSeries: Series = pandas.concat(objs=[openDates, closedDates], axis=0)

    dateToIntervalMap: dict[Timestamp, int] = createDateToIntervalMapping(
        dates=datesSeries
    )

    it: IntervalTree = createIntervalTree(dates=datesDF, map_=dateToIntervalMap)
