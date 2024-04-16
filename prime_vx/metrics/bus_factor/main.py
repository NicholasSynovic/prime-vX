from collections import namedtuple
from datetime import datetime
from typing import List, Tuple

from pandas import DataFrame
from pandas.core.groupby.generic import DataFrameGroupBy
from progress.bar import Bar

from prime_vx.datamodels.metrics.bus_factor import BUS_FACTOR_DF_DATAMODEL
from prime_vx.db import (
    ANNUAL_BUS_FACTOR_DB_TABLE_NAME,
    DAILY_BUS_FACTOR_DB_TABLE_NAME,
    MONTHLY_BUS_FACTOR_DB_TABLE_NAME,
    SIX_MONTH_BUS_FACTOR_DB_TABLE_NAME,
    THREE_MONTH_BUS_FACTOR_DB_TABLE_NAME,
    TWO_MONTH_BUS_FACTOR_DB_TABLE_NAME,
    TWO_WEEK_BUS_FACTOR_DB_TABLE_NAME,
    WEEKLY_BUS_FACTOR_DB_TABLE_NAME,
)
from prime_vx.metrics import createGroups
from prime_vx.metrics.bus_factor import INTERVAL_PAIRS

BUCKET_STOR = namedtuple(
    typename="BUCKET_STOR",
    field_names=[
        DAILY_BUS_FACTOR_DB_TABLE_NAME,
        WEEKLY_BUS_FACTOR_DB_TABLE_NAME,
        TWO_WEEK_BUS_FACTOR_DB_TABLE_NAME,
        MONTHLY_BUS_FACTOR_DB_TABLE_NAME,
        TWO_MONTH_BUS_FACTOR_DB_TABLE_NAME,
        THREE_MONTH_BUS_FACTOR_DB_TABLE_NAME,
        SIX_MONTH_BUS_FACTOR_DB_TABLE_NAME,
        ANNUAL_BUS_FACTOR_DB_TABLE_NAME,
    ],
)

COMMIT_HASH_TO_BUCKET_MAPPING: dict[str, BUCKET_STOR] = {}


def countDevelopers(
    groups: DataFrameGroupBy, frequency: str, minimumContribution: float = 0.2
) -> DataFrame:
    data: dict[str, List[int | float | datetime]] = {
        "bucket": [],
        "bucket_start": [],
        "bucket_end": [],
        "bus_factor": [],
    }

    bucket: int = 1

    with Bar(f"Computing {frequency.replace('_', ' ')}...", max=len(groups)) as bar:
        group: DataFrame
        for _, group in groups:
            contributingDevelopers: List[str] = []

            totalCodeChanged: float = group["delta_kloc"].abs().sum()

            developers: List[str] = group["committer_email"].unique().tolist()

            developer: str
            for developer in developers:
                codeChangedByDeveloper: float = (
                    group[group["committer_email"] == developer]["delta_kloc"]
                    .abs()
                    .sum()
                )

                if codeChangedByDeveloper == 0:
                    break

                if codeChangedByDeveloper / totalCodeChanged >= minimumContribution:
                    contributingDevelopers.append(developer)

            data["bucket"].append(bucket)
            data["bucket_start"].append(
                group["committer_date"].min().to_pydatetime().replace(tzinfo=None)
            )
            data["bucket_end"].append(
                group["committer_date"].max().to_pydatetime().replace(tzinfo=None)
            )
            data["bus_factor"].append(len(contributingDevelopers))

            hash_: str
            for hash_ in group["commit_hash"]:
                setattr(
                    COMMIT_HASH_TO_BUCKET_MAPPING[hash_],
                    frequency,
                    bucket,
                )

            bucket += 1
            bar.next()

    return BUS_FACTOR_DF_DATAMODEL(df=DataFrame(data=data)).df


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

        dfDict[frequency] = countDevelopers(
            groups=group[1],
            frequency=frequency,
        )

    return dfDict
