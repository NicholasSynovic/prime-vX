from datetime import datetime
from typing import List, Tuple

from pandas import DataFrame, Grouper
from pandas.core.groupby.generic import DataFrameGroupBy
from progress.bar import Bar


def computeProductivity(groups: DataFrameGroupBy, datum: Tuple[str, str]) -> DataFrame:
    # TODO: Add doc string

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
            data["bucket_end"].append(group["committerDate"].max().to_pytdatetime())
            data["effort_KLOC"].append(effortKLOC)
            data["effort_LOC"].append(effortLOC)
            data["productivity_KLOC"].append(productivityKLOC)
            data["productivity_LOC"].append(productivityLOC)

            bucket += 1
            bar.next()

    return DataFrame(data=data)


def main(df: DataFrame) -> DataFrame:
    # TODO: Add docstring

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

        df: DataFrame = computeProductivity(groups=groups, datum=datum)
