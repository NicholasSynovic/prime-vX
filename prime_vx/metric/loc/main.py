from typing import List, Tuple

from pandas import DataFrame
from progress.bar import Bar


def computeLOC(loc: int) -> int:
    # TODO: Add docstring
    return loc


def computeKLOC(loc: int) -> float:
    # TODO: Add docstring
    return loc / 1000


def computeDeltaLOC(currentLOC: int, previousLOC: int = 0) -> int:
    # TODO: Add docstring
    return currentLOC - previousLOC


def computeDeltaKLOC(currentLOC: int, previousLOC: int = 0) -> float:
    # TODO: Add docstring
    return computeKLOC(loc=currentLOC) - computeKLOC(loc=previousLOC)


def main(df: DataFrame) -> DataFrame:
    # TODO: Add docstring

    previousLOC: int = 0

    data: dict[str, List[str | int | float]] = {
        "commitHash": [],
        "loc": [],
        "kloc": [],
        "delta_loc": [],
        "delta_kloc": [],
    }

    relevantDF: DataFrame = df[["commitHash", "lineCount"]]

    with Bar(
        "Computing LOC, KLOC, Delta LOC, and Delta KLOC...", max=df.shape[0]
    ) as bar:
        row: Tuple[str, int]
        for row in relevantDF.itertuples(index=False):
            data["commitHash"].append(row[0])
            data["loc"].append(computeLOC(loc=row[1]))
            data["kloc"].append(computeKLOC(loc=row[1]))

            data["delta_loc"].append(
                computeDeltaLOC(currentLOC=row[1], previousLOC=previousLOC)
            )
            data["delta_kloc"].append(
                computeDeltaKLOC(currentLOC=row[1], previousLOC=previousLOC)
            )

            previousLOC = row[1]
            bar.next()

    # TODO: Add datamodel
    return DataFrame(data=data)
