from json import dumps, loads
from pathlib import Path
from typing import List

from pandas import DataFrame
from pyfs import isDirectory, resolvePath, runCommand

from prime_vx.cloc._classes._clocTool import CLOCTool_ABC
from prime_vx.datamodels.cloc import CLOC_DF_DATAMODEL
from prime_vx.exceptions import InvalidDirectoryPath


class CLOC(CLOCTool_ABC):
    def __init__(self, path: Path) -> None:
        self.toolName = "cloc"

        resolvedPath: Path = resolvePath(path=path)

        if isDirectory(path=resolvedPath):
            self.path = resolvedPath
        else:
            raise InvalidDirectoryPath

        self.command = f"{self.toolName} --by-file-by-lang --use-sloccount --json {self.path.__str__()}"

    def compute(self, commitHash: str) -> DataFrame:
        data: dict[str, List] = {"commit_hash": [commitHash]}

        output: str = runCommand(cmd=self.command).stdout.decode().strip()
        jsonData: List = loads(s=output)

        byLangSUM: dict = jsonData["by_lang"]["SUM"]

        data["file_count"] = [byLangSUM["nFiles"]]
        data["line_count"] = [
            byLangSUM["blank"] + byLangSUM["comment"] + byLangSUM["code"]
        ]
        data["blank_line_count"] = [byLangSUM["blank"]]
        data["comment_line_count"] = byLangSUM["comment"]
        data["code_line_count"] = byLangSUM["code"]
        data["tool"] = [self.toolName]
        data["json"] = [dumps(obj=jsonData)]

        return CLOC_DF_DATAMODEL(df=DataFrame(data=data)).df
