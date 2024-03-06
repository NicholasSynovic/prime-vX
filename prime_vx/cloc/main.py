from argparse import Namespace
from pathlib import Path
from typing import List, Tuple

import pandas
from pandas import DataFrame
from progress.bar import Bar

from prime_vx.cloc._classes._clocTool import CLOCTool_ABC
from prime_vx.db.sqlite import VCS_DB
from prime_vx.vcs.git import GitHandler


def extractCommitMetadata(handler: CLOCTool_ABC) -> DataFrame:
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


def main(namespace: Namespace) -> None:
    programInput: dict[str, List[Path]] = dict(namespace._get_kwargs())
    programKeys: List[str] = list(programInput.keys())

    inputKey: str = [key for key in programKeys if "input" in key][0]
    inputKeySplit: List[str] = inputKey.split(sep=".")
    dbPath: Path = programInput[inputKey][0]

    match inputKeySplit[1]:
        case "scc":
            clocHandler: CLOCTool_ABC = GitHandler(path=repositoryPath)
        case _:
            print("Invalid version control system")
            quit(1)

    metadataDF: DataFrame = extractCommitMetadata(handler=vcsHandler)

    metadataDBHandler: VCS_DB = VCS_DB(path=dbPath)
    metadataDBHandler.createMetadata()
    metadataDBHandler.write(df=metadataDF)
