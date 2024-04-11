from json import dumps, loads
from pathlib import Path
from typing import List

from pandas import DataFrame
from pyfs import isDirectory, resolvePath, runCommand

from prime_vx.cloc._classes._clocTool import CLOCTool_ABC
from prime_vx.datamodels.cloc import CLOC_DF_DATAMODEL
from prime_vx.exceptions import InvalidDirectoryPath


class SCC(CLOCTool_ABC):
    def __init__(self, path: Path) -> None:
        self.toolName = "scc"

        resolvedPath: Path = resolvePath(path=path)

        if isDirectory(path=resolvedPath):
            self.path = resolvedPath
        else:
            raise InvalidDirectoryPath

        self.command = f"scc --by-file --min-gen --no-complexity --no-duplicates --format json {self.path.__str__()}"

    def compute(self, commitHash: str) -> CLOC_DF_DATAMODEL:
        data: dict[str, List] = {"commit_hash": [commitHash]}

        output: str = runCommand(cmd=self.command).stdout.decode().strip()
        jsonData: List = loads(s=output)

        data["file_count"] = [sum([len(document["Files"]) for document in jsonData])]
        data["line_count"] = [sum([document["Lines"] for document in jsonData])]
        data["blank_line_count"] = [sum([document["Blank"] for document in jsonData])]
        data["comment_line_count"] = [
            sum([document["Comment"] for document in jsonData])
        ]
        data["code_line_count"] = [sum([document["Code"] for document in jsonData])]
        data["json"] = [dumps(obj=jsonData)]

        df: DataFrame = DataFrame(data=data)

        return CLOC_DF_DATAMODEL(df=df)
