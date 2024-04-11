from datetime import datetime
from pathlib import Path
from typing import Any, List

from pandas import DataFrame
from pyfs import isDirectory, resolvePath, runCommand

from prime_vx.datamodels.vcs import VCS_DF_DATAMODEL
from prime_vx.vcs import VCS_METADATA_KEY_LIST
from prime_vx.vcs._classes._vcsHandler import VCSHandler_ABC


class GitHandler(VCSHandler_ABC):
    def __init__(self, path: Path) -> None:
        resolvedPath: Path = resolvePath(path=path)

        if isDirectory(path=resolvedPath):
            self.path = resolvedPath
        else:
            print("Invalid directory path. Please point path to a directory")
            quit(1)

        self.cmdPrefix: str = f"git --no-pager -C {self.path}"

    def isRepository(self) -> bool:
        code: int = runCommand(
            cmd=f"{self.cmdPrefix} rev-parse --is-inside-work-tree"
        ).returncode

        if code == 0:
            return True
        else:
            return False

    def getCommitHashes(self) -> List[str]:
        hashes: str = runCommand(
            cmd=f"{self.cmdPrefix} log --all --reverse --format='%H'"
        ).stdout.decode()
        hashList: List[str] = hashes.strip().replace("'", "").split(sep="\n")
        return hashList

    def checkoutCommit(self, commitHash: str) -> bool:
        code: int = runCommand(
            cmd=f"{self.cmdPrefix} checkout --force {commitHash}"
        ).returncode

        if code == 0:
            return True
        else:
            return False

    def getCommitMetadata(self, commitHash: str) -> DataFrame:
        values: List[str] = (
            runCommand(
                cmd=f"{self.cmdPrefix} log {commitHash} -n 1 --format='%H,,%T,,%P,,%an,,%ae,,%at,,%cn,,%ce,,%ct,,%d,,%S,,%G?'"
            )
            .stdout.decode()
            .strip()
            .replace("'", "")
            .split(sep=",,")
        )

        metadata: dict[str, Any] = dict(zip(VCS_METADATA_KEY_LIST, values))

        key: str
        value: str
        for key, value in metadata.items():
            metadata[key] = [value]

        metadata["author_date"] = [
            datetime.fromtimestamp(float(metadata["author_date"][0]))
        ]
        metadata["committer_date"] = [
            datetime.fromtimestamp(float(metadata["committer_date"][0]))
        ]

        metadata["vcs"] = ["git"]
        metadata["path"] = [self.path.__str__()]

        df: DataFrame = DataFrame(data=metadata)

        return VCS_DF_DATAMODEL(df=df).df
