from typing import List, Tuple

from pandas import DataFrame
from progress.bar import Bar

from prime_vx.datamodels.metrics.loc import LOC_DF_DATAMODEL

DATA_STOR_DICT: dict[str, List] = {
    "commitHash": [],
    "dateBucket": [],
    "productivity": [],
}


def computeDailyProductivity(loc: int) -> int:
    # TODO: Add docstring
    return loc


def computeWeeklyProductivity(loc: int) -> float:
    # TODO: Add docstring
    return loc / 1000


def computeBiWeeklyProductivity(loc: int) -> float:
    # TODO: Add docstring
    return loc / 1000


def computeMonthlyProductivity(currentLOC: int, previousLOC: int = 0) -> int:
    # TODO: Add docstring
    return currentLOC - previousLOC


def computeTwoMonthProductivity(currentLOC: int, previousLOC: int = 0) -> int:
    # TODO: Add docstring
    return currentLOC - previousLOC


def computeThreeMonthProductivity(currentLOC: int, previousLOC: int = 0) -> int:
    # TODO: Add docstring
    return currentLOC - previousLOC


def computeSixMonthProductivity(currentLOC: int, previousLOC: int = 0) -> int:
    # TODO: Add docstring
    return currentLOC - previousLOC


def computeAnnualProductivity(currentLOC: int, previousLOC: int = 0) -> int:
    # TODO: Add docstring
    return currentLOC - previousLOC


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

    relevantDataVCS: DataFrame = df[
        [
            "commitHash",
            "committerDate",
            "loc",
            "kloc",
        ],
    ]

    print(relevantDataVCS.dtypes)
    print(relevantDataVCS.shape)

    # datum: Tuple[str, int, dict]
    # for datum in data:
    #     with Bar(msg=f"Computing {datum[0]} productivity...", max=-1) as bar:
    #         pass

    pass
