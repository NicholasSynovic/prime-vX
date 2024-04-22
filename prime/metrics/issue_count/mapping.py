from typing import List, Tuple

from pandas import DataFrame, Series
from pandas.core.groupby.generic import DataFrameGroupBy
from progress.bar import Bar

from prime.datamodels.issue_tracker import IT_DF_DATAMODEL
from prime.datamodels.metrics.issue_count import ISSUE_COUNT_MAPPING_DF_DATAMODEL
from prime.db import *
from prime.metrics import createGroups
from prime.metrics.issue_count import BUCKET_STOR, INTERVAL_PAIRS


def issueIDToBucketMapper(df: DataFrame) -> dict[str, BUCKET_STOR]:
    IT_DF_DATAMODEL(df=df)

    issueIDs: Series = df["id"]

    data: dict[str, BUCKET_STOR] = {id_: BUCKET_STOR() for id_ in issueIDs}

    groups: List[Tuple[str, DataFrameGroupBy]] = createGroups(
        df=df,
        intervalPairs=INTERVAL_PAIRS,
        key="date_opened",
    )

    group: Tuple[str, DataFrameGroupBy]
    groupDF: DataFrame
    hash_: str
    for group in groups:
        bucket: int = 1
        frequency: str = group[0]

        with Bar(
            f"Mapping issues IDs to {frequency} table...", max=len(group[1])
        ) as bar:
            for _, groupDF in group[1]:
                for hash_ in groupDF["id"]:
                    frequencyValue: dict[str, int] = {frequency: bucket}
                    data[hash_] = data[hash_]._replace(**frequencyValue)

                bucket += 1
                bar.next()

    return data


def main(df: DataFrame) -> DataFrame:
    data: dict[str, List[str | int]] = {
        "issue_id": [],
        DAILY_ISSUE_COUNT_DB_TABLE_NAME: [],
        WEEKLY_ISSUE_COUNT_DB_TABLE_NAME: [],
        TWO_WEEK_ISSUE_COUNT_DB_TABLE_NAME: [],
        MONTHLY_ISSUE_COUNT_DB_TABLE_NAME: [],
        TWO_MONTH_ISSUE_COUNT_DB_TABLE_NAME: [],
        THREE_MONTH_ISSUE_COUNT_DB_TABLE_NAME: [],
        SIX_MONTH_ISSUE_COUNT_DB_TABLE_NAME: [],
        ANNUAL_ISSUE_COUNT_DB_TABLE_NAME: [],
    }

    chtbm: dict[str, BUCKET_STOR] = issueIDToBucketMapper(df=df)

    data["issue_id"].extend(chtbm.keys())

    datum: BUCKET_STOR
    key: str
    for datum in chtbm.values():
        foo: dict[str, int] = datum._asdict()

        for key in foo.keys():
            data[key].append(foo[key])

    return ISSUE_COUNT_MAPPING_DF_DATAMODEL(df=DataFrame(data=data)).df
