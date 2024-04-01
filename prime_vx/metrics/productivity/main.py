from datetime import datetime
from typing import List, Tuple

from pandas import DataFrame, Grouper
from pandas.core.groupby.generic import DataFrameGroupBy
from progress.bar import Bar

from prime_vx.datamodels.metrics.productivity import PRODUCTIVITY_DF_DATAMODEL


def computeProductivity(groups: DataFrameGroupBy, datum: Tuple[str, str]) -> DataFrame:
    """
    computeProductivity

    Compute the productivity of a project (effort / time) for different time intervals

    :param groups: A DataFrameGroupBy object that is grouped into time intervals
    :type groups: DataFrameGroupBy
    :param datum: Metadata containing the time interval type being analyzed
    :type datum: Tuple[str, str]
    :return: A DataFrame that conforms to the Productivity datamodel
    :rtype: DataFrame
    """
    data: dict[str, List[int | float | datetime]] = {
        "bucket": [],
        "bucket_start": [],
        "bucket_end": [],
        "effort_LOC": [],
        "effort_KLOC": [],
        "productivity_LOC": [],
        "productivity_KLOC": [],
    }

    bucket: int = 1

    with Bar(f"Computing {datum[0]} productivity...", max=len(groups)) as bar:
        group: DataFrame
        for _, group in groups:
            effortLOC: int = group["delta_loc"].abs().sum()
            effortKLOC: float = group["delta_kloc"].abs().sum()
            productivityLOC: float = effortLOC / bucket
            productivityKLOC: float = effortKLOC / bucket

            data["bucket"].append(bucket)
            data["bucket_start"].append(group["committerDate"].min().to_pydatetime())
            data["bucket_end"].append(group["committerDate"].max().to_pydatetime())
            data["effort_KLOC"].append(effortKLOC)
            data["effort_LOC"].append(effortLOC)
            data["productivity_KLOC"].append(productivityKLOC)
            data["productivity_LOC"].append(productivityLOC)

            bucket += 1
            bar.next()

    return PRODUCTIVITY_DF_DATAMODEL(df=DataFrame(data=data)).df


def main(df: DataFrame) -> dict[str, DataFrame]:
    """
    main

    Wrapper to compute the productivity of a project at different time intervals

    :param df: A DataFrame containing relevant information (i.e LOC and commit time)
    :type df: DataFrame
    :return: A dictionary where each key is a time interval (as a string) and each value is a DataFrame of the productivity throughout that time interval
    :rtype: dict[str, DataFrame]
    """

    dfDict: dict[str, DataFrame] = {}

    data: List[Tuple[str, str]] = [
        ("daily", "D"),
        ("weekly", "W"),
        ("two week", "2W"),
        ("monthly", "ME"),
        ("two month", "2ME"),
        ("three month", "QE"),
        ("six month", "2QE"),
        ("annual", "YE"),
    ]

    relevantDataDF: DataFrame = df[
        ["commitHash", "committerDate", "delta_loc", "delta_kloc"]
    ]

    datum: Tuple[str, str]
    for datum in data:
        frequency: str = datum[1]

        groups: DataFrameGroupBy = relevantDataDF.groupby(
            by=Grouper(
                key="committerDate",
                freq=frequency,
            )
        )

        dfDict[datum[0]] = computeProductivity(
            groups=groups,
            datum=datum,
        )

    return dfDict
