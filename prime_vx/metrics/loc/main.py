from typing import List, Tuple

from pandas import DataFrame
from progress.bar import Bar

from prime_vx.datamodels.metrics.loc import LOC_DF_DATAMODEL


def computeLOC(loc: int) -> int:
    """
    computeLOC

    Compute lines of code.

    :param loc: Number of lines of code outputted from CLOC tool
    :type loc: int
    :return: The number of lines of code
    :rtype: int
    """
    return loc


def computeKLOC(loc: int) -> float:
    """
    computeKLOC

    Compute thousands of lines of code (LOC / 1000)

    :param loc: Number of lines of code
    :type loc: int
    :return: loc / 1000
    :rtype: float
    """
    return loc / 1000


def computeDeltaLOC(currentLOC: int, previousLOC: int = 0) -> int:
    """
    computeDeltaLOC

    Given two different lines of code values, compute the difference between
    the two

    :param currentLOC: The most recent CLOC value
    :type currentLOC: int
    :param previousLOC: The CLOC value to compute against, defaults to 0
    :type previousLOC: int, optional
    :return: currentLOC - previousLOC
    :rtype: int
    """
    return currentLOC - previousLOC


def computeDeltaKLOC(currentLOC: int, previousLOC: int = 0) -> float:
    """
    computeDeltaKLOC

    Given two different lines of code values, compute the difference between
    the two in thousands of lines of code

    :param currentLOC: The most recent CLOC value
    :type currentLOC: int
    :param previousLOC: The CLOC value to compute against, defaults to 0
    :type previousLOC: int, optional
    :return: currentLOC - previousLOC
    :rtype: int
    """
    return computeKLOC(loc=currentLOC) - computeKLOC(loc=previousLOC)


def main(df: DataFrame) -> DataFrame:
    """
    main

    Compute LOC, KLOC, and Delta metrics

    :param df: A DataFrame with CLOC information per commit
    :type df: DataFrame
    :return: A DataFrame with the computed CLOC information
    :rtype: DataFrame
    """
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

    df_: DataFrame = DataFrame(data=data)

    return LOC_DF_DATAMODEL(df=df_).df
