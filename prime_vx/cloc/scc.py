from json import loads
from pathlib import Path
from typing import List

from pandas import DataFrame

from prime_vx.cloc._classes._clocTool import CLOCTool_ABC
from prime_vx.shell.fs import isDirectory, resolvePath
from prime_vx.shell.shell import runProgram


class SCC(CLOCTool_ABC):
    """
    SCC

    Class to interface with the SCC tool
    """

    def __init__(self, path: Path) -> None:
        self.toolName = "scc"

        resolvedPath: Path = resolvePath(path=path)

        if isDirectory(path=resolvedPath):
            self.path = resolvedPath
        else:
            print("Invalid directory path. Please point path to a directory")
            quit(1)

        self.command = f"scc --by-file --min-gen --no-complexity --no-duplicates --format json {self.path.__str__()}"

    def compute(self) -> DataFrame:
        # TODO: Validate the output with a Typed Frame
        data: dict[str, List] = {}

        output: str = runProgram(cmd=self.command).stdout.decode().strip()
        jsonData: List[dict[str, int | List]] = loads(s=output)

        data["fileCount"] = [sum([len(document["Files"]) for document in jsonData])]
        data["lineCount"] = [sum([document["Lines"] for document in jsonData])]
        data["blankLineCount"] = [sum([document["Blank"] for document in jsonData])]
        data["commentLineCount"] = [sum([document["Comment"] for document in jsonData])]
        data["codeLineCount"] = [sum([document["Code"] for document in jsonData])]
        data["json"] = [jsonData]

        return DataFrame(data=data)
