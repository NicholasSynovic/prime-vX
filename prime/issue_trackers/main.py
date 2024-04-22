from argparse import Namespace
from typing import List

from pandas import DataFrame
from requests import Response

from prime.db import ISSUE_TRACKER_DB_TABLE_NAME
from prime.db.sqlite import SQLite
from prime.exceptions import InvalidIssueTrackerControl
from prime.issue_trackers._classes._issueTrackerHandler import ITHandler_ABC
from prime.issue_trackers.github import GitHubHandler


def getIssues(handler: ITHandler_ABC) -> DataFrame:
    resps: List[Response] = handler.getResponses()
    return handler.extractIssues(resps=resps)


def main(namespace: Namespace, db: SQLite) -> None:
    programInput: dict[str, List] = dict(namespace._get_kwargs())
    programKeys: List[str] = list(programInput.keys())

    inputKey: str = [key for key in programKeys if "input" in key][0]
    ownerKey: str = [key for key in programKeys if "owner" in key][0]
    repoKey: str = [key for key in programKeys if "repo" in key][0]
    tokenKey: str = [key for key in programKeys if "token" in key][0]

    inputKeySplit: List[str] = inputKey.split(sep=".")

    owner: str = programInput[ownerKey][0]
    repo: str = programInput[repoKey][0]
    token: str = programInput[tokenKey][0]

    match inputKeySplit[1]:
        case "github":
            itHandler: ITHandler_ABC = GitHubHandler(
                owner=owner,
                repo=repo,
                token=token,
            )
        case _:
            raise InvalidIssueTrackerControl()

    metadataDF: DataFrame = getIssues(handler=itHandler)

    db.write(df=metadataDF, tableName=ISSUE_TRACKER_DB_TABLE_NAME)
