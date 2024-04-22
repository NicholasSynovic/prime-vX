import re
from abc import ABCMeta, abstractmethod
from json import dumps, loads
from json.decoder import JSONDecodeError
from pathlib import Path
from typing import List, Protocol, Tuple, runtime_checkable

from pandas import DataFrame
from pyfs import isDirectory, resolvePath, runCommand

from prime.exceptions import InvalidDirectoryPath


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
        temp: dict[str, List] = {
            "language": [],
            "file_count": [],
            "line_count": [],
            "blank_count": [],
            "comment_count": [],
            "code_count": [],
            "file": [],
        }

        clocToolOutput: str = runCommand(cmd=self.command).stdout.decode().strip()

        match self.toolName:
            case "sloccount":
                startingIndex: int = clocToolOutput.find("\n\n\n") + 3
                tsvOutput: str = clocToolOutput[startingIndex:-1]
                perLineSplit: List[str] = tsvOutput.split(sep="\n")

                line: str
                for line in perLineSplit:
                    tsvSplit: List[str] = line.split(sep="\t")
                    temp["line_count"].append(int(tsvSplit[0]))
                    temp["language"].append(tsvSplit[1])
                    temp["file"].append(tsvSplit[2])

                clocToolOutput = temp

            case "loc":
                perLineSplit: List[str] = clocToolOutput.split("\n")
                from pprint import pprint

                pprint(perLineSplit)
                quit()
            case _:
                pass

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
        except TypeError:
            if type(clocToolOutput) == dict:
                outputJSON = clocToolOutput

        outputStr: str = dumps(obj=outputJSON)

        return (outputJSON, outputStr)
