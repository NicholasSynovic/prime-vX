from pathlib import Path
from typing import List, Tuple

from pandas import DataFrame
from pyfs import resolvePath

from prime.cloc._classes._clocTool import CLOCTool, CLOCTool_ABC
from prime.datamodels.cloc import CLOC_DF_DATAMODEL, CLOC_TOOL_DATA


class SLOCCount(CLOCTool, CLOCTool_ABC):
    def __init__(self, path: Path) -> None:
        self.toolName = "sloccount"
        self.command = (
            f"{self.toolName} --wide --details {resolvePath(path=path).__str__()}"
        )

        CLOCTool(toolName=self.toolName, command=self.command, directoryPath=path)
