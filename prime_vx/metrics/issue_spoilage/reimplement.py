from typing import List, Tuple

from intervaltree import IntervalTree
from pandas import DataFrame, Timedelta, Timestamp
from progress.bar import Bar


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
        for i in list(zip(start, end)):
            it.addi(begin=i[0].days, end=i[1].days + 1, data=1)
            bar.next()

    return it


def main(df: DataFrame) -> dict[str, DataFrame]:
    dayZero: Timestamp
    dayN: Timestamp
    dayZero, dayN = identifyDayZero_N(df=df)

    df["date_opened"] = df["date_opened"].fillna(value=dayZero)
    df["date_closed"] = df["date_closed"].fillna(value=dayN)

    timeline: List[int] = list(range((dayN - dayZero).days))

    it: IntervalTree = buildIntervalTree(df=df, dayZero=dayZero)
