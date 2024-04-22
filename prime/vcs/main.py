from argparse import Namespace
from pathlib import Path
from typing import List

import pandas
from pandas import DataFrame
from progress.bar import Bar

from prime.db import VCS_DB_TABLE_NAME
from prime.db.sqlite import SQLite
from prime.exceptions import InvalidVersionControl
from prime.vcs._classes._vcsHandler import VCSHandler_ABC
from prime.vcs.git import GitHandler
from prime.vcs.mercurial import MercurialHandler


def extractCommitMetadata(handler: VCSHandler_ABC) -> DataFrame:
    data: List[DataFrame] = []

    if handler.isRepository() is False:
        print(f"Invalid repository path")
        quit(1)

    hashes: List[str] = handler.getCommitHashes()

    with Bar("Extracting commit metadata...", max=len(hashes)) as bar:
        hash_: str
        for hash_ in hashes:
            df: DataFrame | None = handler.getCommitMetadata(commitHash=hash_)
            if df is None:
                bar.next()
                continue

            data.append(df)
            bar.next()

    return pandas.concat(objs=data, ignore_index=True)


def main(namespace: Namespace, db: SQLite) -> None:
    programInput: dict[str, List[Path]] = dict(namespace._get_kwargs())
    programKeys: List[str] = list(programInput.keys())

    inputKey: str = [key for key in programKeys if "input" in key][0]
    inputKeySplit: List[str] = inputKey.split(sep=".")
    repositoryPath: Path = programInput[inputKey][0]

    match inputKeySplit[1]:
        case "git":
            vcsHandler: VCSHandler_ABC = GitHandler(path=repositoryPath)
        case "hg":
            vcsHandler: VCSHandler_ABC = MercurialHandler(path=repositoryPath)
        case _:
            raise InvalidVersionControl

    metadataDF: DataFrame = extractCommitMetadata(handler=vcsHandler)

    db.write(df=metadataDF, tableName=VCS_DB_TABLE_NAME)
