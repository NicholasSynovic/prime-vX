from datetime import datetime
from pathlib import Path
from typing import Any, List

from pandas import DataFrame
from pyfs import isDirectory, resolvePath, runCommand

from prime.datamodels.vcs import VCS_DF_DATAMODEL
from prime.vcs import VCS_METADATA_KEY_LIST
from prime.vcs._classes._vcsHandler import VCSHandler_ABC


class MercurialHandler(VCSHandler_ABC):
    def __init__(self, path: Path) -> None:
        resolvedPath: Path = resolvePath(path=path)

        if isDirectory(path=resolvedPath):
            self.path = resolvedPath
        else:
            print("Invalid directory path. Please point path to a directory")
            quit(1)

        self.cmdPrefix: str = f"hg --pager never -R {self.path}"

    def isRepository(self) -> bool:
        code: int = runCommand(
            cmd=f"{self.cmdPrefix} --cwd {self.path}"
        ).returncode

        if code == 0:
            return True
        else:
            return False

    def getCommitHashes(self) -> List[str]:
        hashes: str = runCommand(
            cmd=f"{self.cmdPrefix} log --template '{{node}}\n'"
        ).stdout.decode(errors="ignore")
        hashList: List[str] = hashes.strip().replace("'", "").split(sep="\n")
        return hashList

    def checkoutCommit(self, commitHash: str) -> bool:
        code: int = runCommand(
            cmd=f"{self.cmdPrefix} update --clean {commitHash}"
        ).returncode

        if code == 0:
            return True
        else:
            return False

    def getCommitMetadata(self, commitHash: str) -> DataFrame | None:
        values: List[str] = (
            runCommand(
                cmd=f"{self.cmdPrefix} log -r {commitHash} -T '{{node}},,'',,\
                    {{parents}},,{{author|user}},,{{author|email}},,\
                        {{date|hgdate}},,{{author|user}},,{{author|email}},,\{{date|hgdate}},,{{rev}},,{{branch}},,''\n'"
            )
            .stdout.decode(errors="ignore")
            .strip()
            .replace("'", "")
            .split(sep=",,")
        )

        metadata: dict[str, Any] = dict(zip(VCS_METADATA_KEY_LIST, values))

        if len(metadata) == 1:
            return None

        key: str
        value: str
        for key, value in metadata.items():
            metadata[key] = [value]

        metadata["author_date"] = [
            datetime.fromtimestamp(
                float(metadata["author_date"][0].split(" ")[0])
            ).replace(tzinfo=None)
        ]
        metadata["committer_date"] = [
            datetime.fromtimestamp(
                float(metadata["committer_date"][0].split(" ")[0])
            ).replace(tzinfo=None)
        ]

        metadata["vcs"] = ["mercurial"]
        metadata["path"] = [self.path.__str__()]

        df: DataFrame = DataFrame(data=metadata)

        return VCS_DF_DATAMODEL(df=df).df
