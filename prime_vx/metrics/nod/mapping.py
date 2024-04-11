from typing import List, Tuple

from pandas import DataFrame, Series
from pandas.core.groupby.generic import DataFrameGroupBy
from progress.bar import Bar

from prime_vx.datamodels.metrics.nod import DEVELOPER_COUNT_MAPPING_DF_DATAMODEL
from prime_vx.datamodels.vcs import VCS_DF_DATAMODEL
from prime_vx.db import *
from prime_vx.metrics import createGroups
from prime_vx.metrics.nod import BUCKET_STOR, INTERVAL_PAIRS


def commitHashToBucketMapper(df: DataFrame) -> dict[str, BUCKET_STOR]:
    VCS_DF_DATAMODEL(df=df)

    hashes: Series = df["commit_hash"]

    data: dict[str, BUCKET_STOR] = {hash_: BUCKET_STOR() for hash_ in hashes}

    groups: List[Tuple[str, DataFrameGroupBy]] = createGroups(
        df=df,
        intervalPairs=INTERVAL_PAIRS,
    )

    group: Tuple[str, DataFrameGroupBy]
    groupDF: DataFrame
    hash_: str
    for group in groups:
        bucket: int = 1
        frequency: str = group[0]

        with Bar(
            f"Mapping commit hashes to {frequency} table...", max=len(group[1])
        ) as bar:
            for _, groupDF in group[1]:
                for hash_ in groupDF["commit_hash"]:
                    frequencyValue: dict[str, int] = {frequency: bucket}
                    data[hash_] = data[hash_]._replace(**frequencyValue)

                bucket += 1
                bar.next()

    return data


def main(df: DataFrame) -> DataFrame:
    data: dict[str, List[str | int]] = {
        "commit_hash": [],
        DAILY_DEVELOPER_COUNT_DB_TABLE_NAME: [],
        WEEKLY_DEVELOPER_COUNT_DB_TABLE_NAME: [],
        TWO_WEEK_DEVELOPER_COUNT_DB_TABLE_NAME: [],
        MONTHLY_DEVELOPER_COUNT_DB_TABLE_NAME: [],
        TWO_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME: [],
        THREE_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME: [],
        SIX_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME: [],
        ANNUAL_DEVELOPER_COUNT_DB_TABLE_NAME: [],
    }

    chtbm: dict[str, BUCKET_STOR] = commitHashToBucketMapper(df=df)

    data["commit_hash"].extend(chtbm.keys())

    datum: BUCKET_STOR
    key: str
    for datum in chtbm.values():
        foo: dict[str, int] = datum._asdict()

        for key in foo.keys():
            data[key].append(foo[key])

    return DEVELOPER_COUNT_MAPPING_DF_DATAMODEL(df=DataFrame(data=data)).df
