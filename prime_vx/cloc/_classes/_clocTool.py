import re
from abc import ABCMeta, abstractmethod
from json import dumps, loads
from json.decoder import JSONDecodeError
from pathlib import Path
from typing import List, Protocol, Tuple, runtime_checkable

from pandas import DataFrame
from pyfs import isDirectory, resolvePath, runCommand

from prime_vx.exceptions import InvalidDirectoryPath


@runtime_checkable
class CLOCTool_Protocol(Protocol):
    command: str
    path: Path
    toolName: str


class CLOCTool_ABC(CLOCTool_Protocol, metaclass=ABCMeta):
    @abstractmethod
    def compute(self, commitHash: str) -> DataFrame:
        ...


class CLOCTool(CLOCTool_Protocol):
    def __init__(
        self,
        toolName: str,
        command: str,
        directoryPath: Path,
    ) -> None:
        self.command: str = command
        self.toolName: str = toolName
        self.path: Path = Path()

        resolvedDirectoryPath: Path = resolvePath(path=directoryPath)
        if isDirectory(path=resolvedDirectoryPath):
            self.path = resolvedDirectoryPath
        else:
            raise InvalidDirectoryPath

    def runTool(self) -> Tuple[dict | List, str]:
        clocToolOutput: str = runCommand(cmd=self.command).stdout.decode().strip()

        outputJSON: List | dict
        try:
            outputJSON = loads(s=clocToolOutput)
        except JSONDecodeError:
            fixedOutput: str = re.sub(
                pattern=": ,",
                repl=": 0,",
                string=clocToolOutput,
            )
            outputJSON = loads(s=fixedOutput)

        outputStr: str = dumps(obj=outputJSON)

        return (outputJSON, outputStr)
