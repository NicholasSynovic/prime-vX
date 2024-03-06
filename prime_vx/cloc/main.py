from argparse import Namespace
from pathlib import Path
from typing import List

import pandas
from pandas import DataFrame

from prime_vx.datamodels.vcs import VCS_DF_DATAMODEL
from prime_vx.db.sqlite import VCS_DB
from prime_vx.shell.fs import isFile, resolvePath
from prime_vx.vcs.main import DB_TABLE_NAME as VCS_DB_TABLE_NAME


def main(namespace: Namespace) -> None:
    # TODO: Add docstring

    programInput: dict[str, List[Path]] = dict(namespace._get_kwargs())
    programKeys: List[str] = list(programInput.keys())

    inputKey: str = [key for key in programKeys if "input" in key][0]
    inputKeySplit: List[str] = inputKey.split(sep=".")

    dbPath: Path = programInput[inputKey][0]
    resolvedDBPath: Path = resolvePath(path=dbPath)

    if isFile(path=resolvedDBPath):
        pass
    else:
        print(
            "Invalid filepath. Please point to a database created with a PRIME VCS tool."
        )
        quit(1)
