from pathlib import Path
from typing import List, Tuple

from pandas import DataFrame
from pyfs import resolvePath

from prime.cloc._classes._clocTool import CLOCTool, CLOCTool_ABC
from prime.datamodels.cloc import CLOC_DF_DATAMODEL, CLOC_TOOL_DATA


class CLOC(CLOCTool, CLOCTool_ABC):
    def __init__(self, path: Path) -> None:
        self.toolName = "cloc"
        self.command = f"{self.toolName} --by-file --use-sloccount --json {resolvePath(path=path).__str__()}"

        CLOCTool(toolName=self.toolName, command=self.command, directoryPath=path)

    def compute(self, commitHash: str) -> DataFrame:
        data: dict[str, List] = CLOC_TOOL_DATA

        data["commit_hash"].append(commitHash)
        data["tool"].append(self.toolName)

        toolData: Tuple[dict | List, str] = self.runTool()
        jsonDict: dict | List = toolData[0]
        jsonStr: str = toolData[1]

        data["json"].append(jsonStr)

        relevantData: dict = jsonDict["by_lang"]["SUM"]

        data["file_count"].append(relevantData["nFiles"])
        data["blank_line_count"].append(relevantData["blank"])
        data["comment_line_count"].append(relevantData["comment"])
        data["code_line_count"].append(relevantData["code"])

        data["line_count"].append(
            relevantData["blank"] + relevantData["comment"] + relevantData["code"]
        )

        return CLOC_DF_DATAMODEL(df=DataFrame(data=data)).df
