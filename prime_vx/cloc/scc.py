from pathlib import Path
from typing import List, Tuple

from pandas import DataFrame
from pyfs import resolvePath

from prime_vx.cloc._classes._clocTool import CLOCTool, CLOCTool_ABC
from prime_vx.datamodels.cloc import CLOC_DF_DATAMODEL, CLOC_TOOL_DATA


class SCC(CLOCTool, CLOCTool_ABC):
    def __init__(self, path: Path) -> None:
        self.toolName = "scc"
        self.command = f"{self.toolName} --by-file --min-gen --no-complexity --no-duplicates --format json {resolvePath(path=path).__str__()}"

        CLOCTool(toolName=self.toolName, command=self.command, directoryPath=path)

    def compute(self, commitHash: str) -> DataFrame:
        data: dict[str, List] = CLOC_TOOL_DATA

        data["commit_hash"].append(commitHash)
        data["tool"].append(self.toolName)

        toolData: Tuple[dict | List, str] = self.runTool()
        jsonDict: dict | List = toolData[0]
        jsonStr: str = toolData[1]

        data["json"].append(jsonStr)

        fileCount: int = sum(
            [len(document["Files"]) for document in jsonDict],
        )

        lineCount: int = sum(
            [document["Lines"] for document in jsonDict],
        )

        blankLineCount: int = sum(
            [document["Blank"] for document in jsonDict],
        )

        commentLineCount: int = sum(
            [document["Comment"] for document in jsonDict],
        )

        codeLineCount: int = sum(
            [document["Code"] for document in jsonDict],
        )

        data["file_count"].append(fileCount)
        data["blank_line_count"].append(blankLineCount)
        data["comment_line_count"].append(commentLineCount)
        data["code_line_count"].append(codeLineCount)
        data["line_count"].append(lineCount)

        return CLOC_DF_DATAMODEL(df=DataFrame(data=data)).df
