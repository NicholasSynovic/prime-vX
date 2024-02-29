from os.path import abspath, isdir
from pathlib import Path
from typing import List

from prime_vx.shell.shell import resolvePath, runProgram
from prime_vx.vcs._vcsHandler import VCSHandler_ABC


class GitHandler(VCSHandler_ABC):
    def __init__(self, path: Path) -> None:
        resolvedPath: Path = resolvePath(path=path)
        if isdir(s=resolvedPath):
            self.path = resolvedPath
        else:
            print("Invalid git repository. Please point path to a directory")
            quit(1)

        self.gitPrefix: str = f"git --no-pager -C {self.path}"

    def isRepository(self) -> bool:
        code: int = runProgram(
            cmd=f"{self.gitPrefix} rev-parse --is-inside-work-tree"
        ).returncode

        if code == 0:
            return True
        else:
            return False

    def getCommitHashes(self) -> List[str]:
        hashes: str = runProgram(
            cmd=f"{self.gitPrefix} log --reverse --format='%H'"
        ).stdout.decode()
        hashList: List[str] = hashes.strip().replace("'", "").split(sep="\n")
        return hashList

    def checkoutCommit(self, commitHash: str) -> bool:
        return True

    def getCommitMetadata(self, commitHash: str) -> dict[str, str]:
        return {}


p = Path("~/Documents/projects/scratch/numpy")
gh: GitHandler = GitHandler(path=p)

print(resolvePath(path=p))

print(gh.path)
print(gh.isRepository())
print(gh.getCommitHashes())
