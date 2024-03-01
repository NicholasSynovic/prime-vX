from os.path import abspath, expanduser, isdir, isfile
from pathlib import Path
from subprocess import PIPE, CompletedProcess, run


def runProgram(cmd: str) -> CompletedProcess[bytes]:
    execution: CompletedProcess[bytes] = run(
        args=cmd.split(sep=" "),
        stdout=PIPE,
        stderr=PIPE,
    )
    return execution


def resolvePath(path: Path) -> Path:
    return Path(abspath(path=expanduser(path=path)))


def isFile(path: Path) -> bool:
    return isfile(path=resolvePath(path=path))


def isDirectory(path: Path) -> bool:
    return isdir(s=resolvePath(path=path))
