from collections import namedtuple
from datetime import datetime
from typing import List, Tuple

from pandas import DataFrame
from pandas.core.groupby.generic import DataFrameGroupBy
from progress.bar import Bar

from prime.datamodels.metrics.productivity import PRODUCTIVITY_DF_DATAMODEL
from prime.db import (
    ANNUAL_PRODUCTIVITY_DB_TABLE_NAME,
    DAILY_PRODUCTIVITY_DB_TABLE_NAME,
    MONTHLY_PRODUCTIVITY_DB_TABLE_NAME,
    SIX_MONTH_PRODUCTIVITY_DB_TABLE_NAME,
    THREE_MONTH_PRODUCTIVITY_DB_TABLE_NAME,
    TWO_MONTH_PRODUCTIVITY_DB_TABLE_NAME,
    TWO_WEEK_PRODUCTIVITY_DB_TABLE_NAME,
    WEEKLY_PRODUCTIVITY_DB_TABLE_NAME,
)
from prime.metrics import createGroups
from prime.metrics.productivity import INTERVAL_PAIRS

BUCKET_STOR = namedtuple(
    typename="BUCKET_STOR",
    field_names=[
        DAILY_PRODUCTIVITY_DB_TABLE_NAME,
        WEEKLY_PRODUCTIVITY_DB_TABLE_NAME,
        TWO_WEEK_PRODUCTIVITY_DB_TABLE_NAME,
        MONTHLY_PRODUCTIVITY_DB_TABLE_NAME,
        TWO_MONTH_PRODUCTIVITY_DB_TABLE_NAME,
        THREE_MONTH_PRODUCTIVITY_DB_TABLE_NAME,
        SIX_MONTH_PRODUCTIVITY_DB_TABLE_NAME,
        ANNUAL_PRODUCTIVITY_DB_TABLE_NAME,
    ],
)

COMMIT_HASH_TO_BUCKET_MAPPING: dict[str, BUCKET_STOR] = {}


def computeProductivity(groups: DataFrameGroupBy, frequency: str) -> DataFrame:
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

    with Bar(
        f"Computing {frequency.replace('_', ' ')}...", max=len(groups)
    ) as bar:
        group: DataFrame
        for _, group in groups:
            effortLOC: int = group["delta_loc"].abs().sum()
            effortKLOC: float = group["delta_kloc"].abs().sum()
            productivityLOC: float = effortLOC / bucket
            productivityKLOC: float = effortKLOC / bucket

            data["bucket"].append(bucket)
            data["bucket_start"].append(
                group["committer_date"]
                .min()
                .to_pydatetime()
                .replace(tzinfo=None)
            )
            data["bucket_end"].append(
                group["committer_date"]
                .max()
                .to_pydatetime()
                .replace(tzinfo=None)
            )
            data["effort_KLOC"].append(effortKLOC)
            data["effort_LOC"].append(effortLOC)
            data["productivity_KLOC"].append(productivityKLOC)
            data["productivity_LOC"].append(productivityLOC)

            hash_: str
            for hash_ in group["commit_hash"]:
                setattr(
                    COMMIT_HASH_TO_BUCKET_MAPPING[hash_],
                    frequency,
                    bucket,
                )

            bucket += 1
            bar.next()

    return PRODUCTIVITY_DF_DATAMODEL(df=DataFrame(data=data)).df


def main(df: DataFrame) -> dict[str, DataFrame]:
    global COMMIT_HASH_TO_BUCKET_MAPPING

    dfDict: dict[str, DataFrame] = {}

    hashes: List[str] = df["commit_hash"].to_list()
    COMMIT_HASH_TO_BUCKET_MAPPING = {hash_: BUCKET_STOR for hash_ in hashes}

    groups: List[Tuple[str, DataFrameGroupBy]] = createGroups(
        df=df,
        intervalPairs=INTERVAL_PAIRS,
    )

    group: Tuple[str, DataFrameGroupBy]
    for group in groups:
        frequency: str = group[0]

        dfDict[frequency] = computeProductivity(
            groups=group[1],
            frequency=frequency,
        )

    return dfDict
