from pathlib import Path
from typing import List, Tuple

from pandas import DataFrame
from pyfs import resolvePath

from prime.cloc._classes._clocTool import CLOCTool, CLOCTool_ABC
from prime.datamodels.cloc import CLOC_DF_DATAMODEL, CLOC_TOOL_DATA


class LOC(CLOCTool, CLOCTool_ABC):
    def __init__(self, path: Path) -> None:
        self.toolName = "loc"
        self.command = f"{self.toolName} --files {resolvePath(path=path).__str__()}"

        CLOCTool(toolName=self.toolName, command=self.command, directoryPath=path)

    def compute(self, commitHash: str) -> DataFrame:
        # TODO: Implement this
        data: dict[str, List] = CLOC_TOOL_DATA
        return CLOC_DF_DATAMODEL(df=DataFrame(data=data)).df
