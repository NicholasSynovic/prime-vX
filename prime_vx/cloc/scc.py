from json import dumps, loads
from pathlib import Path
from typing import List

from pandas import DataFrame

from prime_vx.cloc._classes._clocTool import CLOCTool_ABC
from prime_vx.datamodels.cloc import CLOC_DF_DATAMODEL
from prime_vx.exceptions import InvalidDirectoryPath
from prime_vx.shell.fs import isDirectory, resolvePath
from prime_vx.shell.shell import runProgram


class SCC(CLOCTool_ABC):
    """
    SCC

    Class to interface with the SCC tool
    """

    def __init__(self, path: Path) -> None:
        """
        __init__

        Initialize class and point it at a specific repository

        :param path: Path to repository to compute CLOC
        :type path: Path
        """

        self.toolName = "scc"

        resolvedPath: Path = resolvePath(path=path)

        if isDirectory(path=resolvedPath):
            self.path = resolvedPath
        else:
            raise InvalidDirectoryPath

        self.command = f"scc --by-file --min-gen --no-complexity --no-duplicates --format json {self.path.__str__()}"

    def compute(self, commitHash: str) -> CLOC_DF_DATAMODEL:
        data: dict[str, List] = {"commitHash": [commitHash]}

        output: str = runProgram(cmd=self.command).stdout.decode().strip()
        jsonData: List = loads(s=output)

        data["fileCount"] = [sum([len(document["Files"]) for document in jsonData])]
        data["lineCount"] = [sum([document["Lines"] for document in jsonData])]
        data["blankLineCount"] = [sum([document["Blank"] for document in jsonData])]
        data["commentLineCount"] = [sum([document["Comment"] for document in jsonData])]
        data["codeLineCount"] = [sum([document["Code"] for document in jsonData])]
        data["json"] = [dumps(obj=jsonData)]

        df: DataFrame = DataFrame(data=data)

        return CLOC_DF_DATAMODEL(df=df)
