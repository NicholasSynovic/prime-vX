from typing import List, Tuple

from pandas import DataFrame, Grouper
from pandas.core.groupby.generic import DataFrameGroupBy
from progress.bar import Bar

from prime_vx.datamodels.metrics.loc import LOC_DF_DATAMODEL

DATA_STOR_DICT: dict[str, List] = {
    "commitHash": [],
    "startDateBucket": [],
    "endDateBucket": [],
    "productivity": [],
}


def computeProductivity(
    groups: DataFrameGroupBy, datum: Tuple[str, str, dict]
) -> DataFrame:
    with Bar(f"Computing {datum[0]} productivity...", max=len(groups)) as bar:
        group: DataFrame
        for _, group in groups:
            print(group["delta_loc"].abs().sum())
            quit()
            bar.next()


def main(df: DataFrame) -> DataFrame:
    # TODO: Add docstring

    data: List[Tuple[str, str, dict]] = [
        ("daily", "D", DATA_STOR_DICT),
        ("weekly", "W", DATA_STOR_DICT),
        ("two week", "2W", DATA_STOR_DICT),
        ("monthly", "ME", DATA_STOR_DICT),
        ("two month", "2ME", DATA_STOR_DICT),
        ("three month", "QE", DATA_STOR_DICT),
        ("six month", "2QE", DATA_STOR_DICT),
        ("annual", "YE", DATA_STOR_DICT),
    ]

    relevantDataDF: DataFrame = df[
        ["commitHash", "committerDate", "delta_loc", "delta_kloc"]
    ]

    datum: Tuple[str, str, dict]
    for datum in data:
        frequency: str = datum[1]

        groups: DataFrameGroupBy = relevantDataDF.groupby(
            by=Grouper(
                key="committerDate",
                freq=frequency,
            )
        )

        tempDF: DataFrame = computeProductivity(groups=groups, datum=datum)
