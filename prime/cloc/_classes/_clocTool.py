import re
from abc import ABCMeta, abstractmethod
from json import dumps, loads
from json.decoder import JSONDecodeError
from pathlib import Path
from typing import List, Protocol, Tuple, runtime_checkable

from pandas import DataFrame
from pyfs import isDirectory, resolvePath, runCommand

from prime.datamodels.cloc import CLOC_TOOL_JSON
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
        clocToolOutput: str | dict[str, List[str | int]] = (
            runCommand(cmd=self.command).stdout.decode().strip()
        )

        match self.toolName:
            case "sloccount":
                temp: dict[str, List[str | int]] = CLOC_TOOL_JSON

                startingIndex: int = clocToolOutput.find("\n\n\n") + 3
                tsvOutput: str = clocToolOutput[startingIndex:-1]
                perLineSplit: List[str] = tsvOutput.split(sep="\n")

                line: str
                for line in perLineSplit:
                    tsvSplit: List[str] = line.split(sep="\t")
                    temp["code_line_count"].append(int(tsvSplit[0]))
                    temp["language"].append(tsvSplit[1])
                    temp["file"].append(tsvSplit[3])

                clocToolOutput = temp

            case "loc":
                temp: dict[str, List[str | int]] = CLOC_TOOL_JSON

                perLineSplit: List[List[str]] = [
                    line.split()
                    for line in clocToolOutput.replace("-", "").split("\n")[3:-2]
                    if len(line) > 0
                ]

                line: List[str]
                for line in perLineSplit:
                    temp["language"].append(line[0])
                    try:
                        temp["file_count"].append(int(line[1]))
                    except ValueError:
                        line[0] = f"{line[0]} {line[1]}"
                        temp["language"][-1] = line[0]
                        del line[1]

                    temp["line_count"].append(int(line[2]))
                    temp["blank_line_count"].append(int(line[3]))
                    temp["comment_line_count"].append(int(line[4]))
                    temp["code_line_count"].append(int(line[5]))

                clocToolOutput = temp

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
