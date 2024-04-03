from argparse import Namespace
from pathlib import Path
from typing import List

import pandas
from pandas import DataFrame
from progress.bar import Bar

from prime_vx.datamodels.issues import ISSUE_DF_DATAMODEL
from prime_vx.db import ISSUES_DB_TABLE_NAME
from prime_vx.db.sqlite import SQLite
from prime_vx.exceptions import InvalidIssueTrackerControl
from prime_vx.issue_trackers._classes._issueTrackerHandler import ITHandler_ABC
from prime_vx.issue_trackers.github import GitHubHandler


def getIssues(handler: ITHandler_ABC) -> DataFrame:
    data: List[DataFrame] = []

    if handler.isRepository() is False:
        print(f"Invalid repository path")
        quit(1)

    hashes: List[str] = handler.getCommitHashes()

    with Bar("Extracting commit metadata...", max=len(hashes)) as bar:
        hash_: str
        for hash_ in hashes:
            data.append(handler.getCommitMetadata(commitHash=hash_).df)
            bar.next()

    df: DataFrame = pandas.concat(objs=data, ignore_index=True)

    validateDF: ISSUE_DF_DATAMODEL = ISSUE_DF_DATAMODEL(df=df)

    return validateDF.df


def main(namespace: Namespace, db: SQLite) -> None:
    programInput: dict[str, List[Path]] = dict(namespace._get_kwargs())
    programKeys: List[str] = list(programInput.keys())

    inputKey: str = [key for key in programKeys if "input" in key][0]
    inputKeySplit: List[str] = inputKey.split(sep=".")
    repositoryPath: Path = programInput[inputKey][0]

    match inputKeySplit[1]:
        case "git":
            vcsHandler: ITHandler_ABC = GitHubHandler(path=repositoryPath)
        case _:
            raise InvalidIssueTrackerControl()

    metadataDF: DataFrame = getIssues(handler=vcsHandler)

    db.write(df=metadataDF, tableName=ISSUES_DB_TABLE_NAME)
