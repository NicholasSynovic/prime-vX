from typing import List, Tuple

from pandas import DataFrame
from progress.bar import Bar

from prime_vx.datamodels.metrics.loc import LOC_DF_DATAMODEL


def computeLOC(loc: int) -> int:
    return loc


def computeKLOC(loc: int) -> float:
    return loc / 1000


def computeDeltaLOC(currentLOC: int, previousLOC: int = 0) -> int:
    return currentLOC - previousLOC


def computeDeltaKLOC(currentLOC: int, previousLOC: int = 0) -> float:
    return computeKLOC(loc=currentLOC) - computeKLOC(loc=previousLOC)


def main(df: DataFrame) -> DataFrame:
    previousLOC: int = 0

    data: dict[str, List[str | int | float]] = {
        "commit_hash": [],
        "loc": [],
        "kloc": [],
        "delta_loc": [],
        "delta_kloc": [],
    }

    relevantDF: DataFrame = df[["commit_hash", "line_count"]]

    with Bar(
        "Computing LOC, KLOC, Delta LOC, and Delta KLOC...", max=df.shape[0]
    ) as bar:
        row: Tuple[str, int]
        for row in relevantDF.itertuples(index=False):
            data["commit_hash"].append(row[0])
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

    df_: DataFrame = DataFrame(data=data)

    return LOC_DF_DATAMODEL(df=df_).df
