from argparse import Namespace
from pathlib import Path
from typing import List

from pandas import DataFrame

from prime_vx.cloc._classes._clocTool import CLOCTool_ABC
from prime_vx.cloc.scc import SCC
from prime_vx.datamodels.vcs import VCS_DF_DATAMODEL
from prime_vx.db.sqlite import VCS_DB
from prime_vx.shell.fs import isFile, resolvePath
from prime_vx.vcs._classes._vcsHandler import VCSHandler_ABC
from prime_vx.vcs.git import GitHandler


def computeCLOC(
    df: DataFrame,
    tool: CLOCTool_ABC,
    vcs: VCSHandler_ABC,
) -> DataFrame:
    print(df.columns)


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

    vcsDB: VCS_DB = VCS_DB(path=resolvedDBPath)
    vcsDF: DataFrame = vcsDB.readTable(tdf=VCS_DF_DATAMODEL)

    relevantColumnsDF: DataFrame = vcsDF[["commitHash", "vcs", "path"]]

    capturedVCS: List = relevantColumnsDF["vcs"].unique()
    capturedPath: List = relevantColumnsDF["path"].unique()

    if len(capturedVCS) > 1:
        print("Too many VCS captured in single database table")
        quit(1)

    if len(capturedPath) > 1:
        print("Too many repositories captured in single database table")
        quit(1)

    repositoryPath: Path = Path(capturedPath[0])

    # Create instance of the CLOC tool for analysis from Namespace
    match inputKeySplit[1]:
        case "scc":
            tool: CLOCTool_ABC = SCC(path=repositoryPath)
        case _:
            print("Invalid tool option")
            quit(1)

    # Create instance of VCS handler from DB
    match capturedVCS[0]:
        case "git":
            vcsHandler: VCSHandler_ABC = GitHandler(path=repositoryPath)
        case _:
            print("Invalid VCS")
            quit(1)

    computeCLOC(df=relevantColumnsDF, tool=tool, vcs=vcsHandler)


main(Namespace(**{"cloc.scc.input": [Path("../test.db")]}))
