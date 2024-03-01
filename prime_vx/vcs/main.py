from argparse import Namespace
from pathlib import Path
from typing import List, Tuple

import pandas
from pandas import DataFrame
from progress.bar import Bar

from prime_vx.db.sqlite import VCS_DB
from prime_vx.vcs._classes._vcsHandler import VCSHandler_ABC
from prime_vx.vcs.git import GitHandler


def collectData(handler: VCSHandler_ABC) -> DataFrame:
    data: List[DataFrame] = []

    if handler.isRepository() is False:
        print(f"Invalid repository path")
        quit(1)

    hashes: List[str] = handler.getCommitHashes()

    with Bar("Extracting commit metadata...", max=len(hashes)) as bar:
        hash_: str
        for hash_ in hashes:
            data.append(handler.getCommitMetadata(commitHash=hash_))
            bar.next()

    df: DataFrame = pandas.concat(objs=data, ignore_index=True)

    return df


def main(namespace: Namespace) -> None:
    programInput: List[Tuple[str, List[Path]]] = namespace._get_kwargs()
    firstParameterSplit: List[str] = programInput[0][0].split(sep=".")

    match firstParameterSplit[1]:
        case "git":
            vcsHandler: VCSHandler_ABC = GitHandler(path=programInput[0][1][0])
        case _:
            print("Invalid version control system")
            quit(1)

    df: DataFrame = collectData(handler=vcsHandler)

    dbHandler: VCS_DB = VCS_DB(path=programInput[1][1][0])
    dbHandler.createMetadata()
    dbHandler.write(df=df)
