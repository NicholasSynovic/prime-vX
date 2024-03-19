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


def main(df: DataFrame) -> DataFrame:
    # TODO: Add docstring

    data: List[Tuple[str, int, dict]] = [
        ("daily", 1, DATA_STOR_DICT),
        ("weekly", 7, DATA_STOR_DICT),
        ("two week", 14, DATA_STOR_DICT),
        ("monthly", -1, DATA_STOR_DICT),
        ("two month", -2, DATA_STOR_DICT),
        ("three month", -3, DATA_STOR_DICT),
        ("six month", -6, DATA_STOR_DICT),
        ("annual", -12, DATA_STOR_DICT),
    ]

    relevantDataDF: DataFrame = df[
        ["commitHash", "committerDate", "delta_loc", "delta_kloc"]
    ]

    datum: Tuple[str, int, dict]
    for datum in data:
        frequency: str = "D"

        if datum[1] > 0:
            match datum[1]:
                case 7:
                    frequency = "W"
                case 14:
                    frequency = "2W"
                case _:
                    # TODO: Add exception
                    pass
        else:
            match datum[1]:
                case -1:
                    frequency = "ME"
                case -2:
                    frequency = "2ME"
                    pass
                case -3:
                    frequency = "QE"
                case -6:
                    frequency = "2QE"
                case -12:
                    frequency = "YE"
                case _:
                    # TODO: Add exception
                    pass

        groups: DataFrameGroupBy = relevantDataDF.groupby(
            by=Grouper(
                key="committerDate",
                freq=frequency,
            )
        )

        with Bar(f"Computing {datum[0]} productivity...", max=len(groups)) as bar:
            group: DataFrame
            for _, group in groups:
                bar.next()
