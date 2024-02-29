from os.path import abspath, expanduser
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
