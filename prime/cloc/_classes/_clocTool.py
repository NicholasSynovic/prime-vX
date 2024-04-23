import re
from abc import ABCMeta, abstractmethod
from json import dumps, loads
from json.decoder import JSONDecodeError
from pathlib import Path
from typing import List, Protocol, Tuple, runtime_checkable

from jsonschema import Draft202012Validator
from numpy import array
from pandas import DataFrame
from pyfs import isDirectory, resolvePath, runCommand

from prime.datamodels.cloc import CLOC_TOOL_JSON, JSON_SCHEMA
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

    def _addDataToJSON(
        self,
        blankLines: List[int],
        codeLines: List[int],
        commentLines: List[int],
        files: List[str],
        languages: List[str],
        lines: List[int],
    ) -> dict[str, str | int]:
        json: dict[str, List[str | int]] = CLOC_TOOL_JSON

        json["blank_line_count"] = blankLines
        json["code_line_count"] = codeLines
        json["comment_line_count"] = commentLines
        json["files"] = files
        json["languages"] = languages
        json["line_count"] = lines

        Draft202012Validator(schema=JSON_SCHEMA).validate(json)

        return json

    def clocFormatter(self, toolData: str) -> dict[str, str | int]:
        badKeys: List[str] = ["header", "SUM"]

        data: dict[str, str | int] = loads(s=toolData)

        filesStr: set[str] = set(data.keys()).difference(badKeys)

        blankLines: List[int] = [data[key]["blank"] for key in filesStr]
        codeLines: List[int] = [data[key]["code"] for key in filesStr]
        commentLines: List[str] = [data[key]["comment"] for key in filesStr]
        files: List[str] = [str(resolvePath(path=Path(file))) for file in filesStr]
        languages: List[str] = [data[key]["language"] for key in filesStr]
        lines: List[int] = (
            array([blankLines, codeLines, commentLines]).sum(axis=0).tolist()
        )

        return self._addDataToJSON(
            blankLines=blankLines,
            codeLines=codeLines,
            commentLines=commentLines,
            files=files,
            languages=languages,
            lines=lines,
        )

    def goclocFormatter(self, toolData: str) -> dict[str, str | int]:
        data: dict[str, str | int] = loads(s=toolData)["files"]

        blankLines: List[int] = [datum["blank"] for datum in data]
        codeLines: List[int] = [datum["code"] for datum in data]
        commentLines: List[str] = [datum["comment"] for datum in data]
        files: List[str] = [
            str(resolvePath(path=Path(datum["name"]))) for datum in data
        ]
        languages: List[str] = [datum["language"] for datum in data]
        lines: List[int] = (
            array([blankLines, codeLines, commentLines]).sum(axis=0).tolist()
        )

        return self._addDataToJSON(
            blankLines=blankLines,
            codeLines=codeLines,
            commentLines=commentLines,
            files=files,
            languages=languages,
            lines=lines,
        )

    def sccFormatter(self, toolData: str) -> dict[str, str | int]:
        outputJSON: dict = CLOC_TOOL_JSON
        jsonStor: List[dict] = []
        data: dict[str, str | int] = loads(s=toolData)

        fileData: List[List[dict]] = [data[idx]["Files"] for idx in range(len(data))]

        fileDatum: List[dict]
        for fileDatum in fileData:
            blankLines: List[int] = [int(datum["Blank"]) for datum in fileDatum]

            codeLines: List[int] = [int(datum["Code"]) for datum in fileDatum]
            commentLines: List[str] = [int(datum["Comment"]) for datum in fileDatum]
            files: List[str] = [
                str(resolvePath(path=Path(datum["Location"]))) for datum in fileDatum
            ]
            languages: List[str] = [datum["Language"] for datum in fileDatum]
            lines: List[int] = (
                array([blankLines, codeLines, commentLines]).sum(axis=0).tolist()
            )

            jsonStor.append(
                self._addDataToJSON(
                    blankLines=blankLines,
                    codeLines=codeLines,
                    commentLines=commentLines,
                    files=files,
                    languages=languages,
                    lines=lines,
                )
            )

        # TODO: Really slow code... needs to be updated
        data: dict
        for data in jsonStor:
            for key in data.keys():
                outputJSON[key].extend(data[key])

        Draft202012Validator(schema=JSON_SCHEMA).validate(outputJSON)

        return outputJSON

    def sloccountFormatter(self, toolData: str) -> dict[str, str | int]:
        startingIndex: int = toolData.find("\n\n\n") + 3
        tsvOutput: str = toolData[startingIndex:-1]
        data: List[str] = [line.split(sep="\t") for line in tsvOutput.split(sep="\n")]

        blankLines: List[int] = [0] * len(data)
        codeLines: List[int] = [int(datum[0]) for datum in data]
        commentLines: List[str] = [0] * len(data)
        files: List[str] = [str(resolvePath(path=Path(datum[3]))) for datum in data]
        languages: List[str] = [datum[1] for datum in data]
        lines: List[int] = (
            array([blankLines, codeLines, commentLines]).sum(axis=0).tolist()
        )

        return self._addDataToJSON(
            blankLines=blankLines,
            codeLines=codeLines,
            commentLines=commentLines,
            files=files,
            languages=languages,
            lines=lines,
        )

    def runTool(self) -> Tuple[dict | List, str]:
        clocToolOutput: str | dict[str, List[str | int]] = (
            runCommand(cmd=self.command).stdout.decode().strip()
        )

        json: dict[str, str | int]
        match self.toolName:
            case "cloc":
                json = self.clocFormatter(toolData=clocToolOutput)

            case "gocloc":
                json = self.goclocFormatter(toolData=clocToolOutput)

            case "scc":
                json = self.sccFormatter(toolData=clocToolOutput)

            case "sloccount":
                json = self.sloccountFormatter(toolData=clocToolOutput)

            case _:
                pass

        jsonStr: str = dumps(obj=json)

        return (json, jsonStr)
