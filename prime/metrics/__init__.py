from typing import List, Tuple

from pandas import DataFrame, Grouper
from pandas.core.groupby.generic import DataFrameGroupBy
from progress.bar import Bar


def createGroups(
    df: DataFrame,
    intervalPairs: List[Tuple[str, str]],
    key: str = "committer_date",
) -> List[Tuple[str, DataFrameGroupBy]]:
    dfs: List[Tuple[str, DataFrameGroupBy]] = []

    with Bar(
        "Computing groups by time interval...", max=len(intervalPairs)
    ) as bar:
        pair: Tuple[str, str]
        for pair in intervalPairs:
            group: DataFrameGroupBy = df.groupby(
                by=Grouper(
                    key=key,
                    freq=pair[1],
                ),
            )

            dfs.append((pair[0], group))
            bar.next()

    return dfs
