from argparse import Namespace
from pathlib import Path
from typing import List

import pandas
from pandas import DataFrame
from progress.bar import Bar

from prime_vx.datamodels.vcs import VCS_DF_DATAMODEL
from prime_vx.db import VCS_DB_TABLE_NAME
from prime_vx.db.sqlite import SQLite
from prime_vx.exceptions import InvalidVersionControl
from prime_vx.vcs._classes._vcsHandler import VCSHandler_ABC
from prime_vx.vcs.git import GitHandler


def extractCommitMetadata(handler: VCSHandler_ABC) -> DataFrame:
    """
    extractCommitMetadata

    Interface to a version control system (VCS) handler to collect information from the VCS about a repository

    :param handler: An implementation of VCSHandler_ABC
    :type handler: VCSHandler_ABC
    :return: A DataFrame of the information of all commits
    :rtype: DataFrame
    """
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

    validateDF: VCS_DF_DATAMODEL = VCS_DF_DATAMODEL(df=df)

    return validateDF.df


def main(namespace: Namespace, db: SQLite) -> None:
    programInput: dict[str, List[Path]] = dict(namespace._get_kwargs())
    programKeys: List[str] = list(programInput.keys())

    inputKey: str = [key for key in programKeys if "input" in key][0]
    inputKeySplit: List[str] = inputKey.split(sep=".")
    repositoryPath: Path = programInput[inputKey][0]

    match inputKeySplit[1]:
        case "git":
            vcsHandler: VCSHandler_ABC = GitHandler(path=repositoryPath)
        case _:
            raise InvalidVersionControl

    metadataDF: DataFrame = extractCommitMetadata(handler=vcsHandler)

    db.write(df=metadataDF, tableName=VCS_DB_TABLE_NAME)
