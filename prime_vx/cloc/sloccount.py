from pathlib import Path
from typing import List, Tuple

from pandas import DataFrame
from pyfs import resolvePath

from prime_vx.cloc import CLOC_TOOL_DATA
from prime_vx.cloc._classes._clocTool import CLOCTool, CLOCTool_ABC
from prime_vx.datamodels.cloc import CLOC_DF_DATAMODEL


class SLOCCount(CLOCTool, CLOCTool_ABC):
    def __init__(self, path: Path) -> None:
        self.toolName = "sloccount"
        self.command = (
            f"{self.toolName} --wide --details {resolvePath(path=path).__str__()}"
        )

        CLOCTool(toolName=self.toolName, command=self.command, directoryPath=path)

    def compute(self, commitHash: str) -> DataFrame:
        data: dict[str, List] = CLOC_TOOL_DATA

        data["commit_hash"].append(commitHash)
        data["tool"].append(self.toolName)
        data["blank_line_count"].append(-1)
        data["comment_line_count"].append(-1)
        data["code_line_count"].append(-1)

        toolData: Tuple[dict | List, str] = self.runTool()
        jsonDict: dict | List = toolData[0]
        jsonStr: str = toolData[1]

        data["json"].append(jsonStr)

        fileCount: int = len(jsonDict["file"])
        lineCount: int = sum(jsonDict["line_count"])

        data["file_count"].append(fileCount)
        data["line_count"].append(lineCount)

        return CLOC_DF_DATAMODEL(df=DataFrame(data=data)).df
