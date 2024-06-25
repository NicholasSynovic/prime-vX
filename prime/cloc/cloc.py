from pathlib import Path

from pyfs import resolvePath

from prime.cloc._classes._clocTool import CLOCTool


class CLOC(CLOCTool):
    def __init__(self, path: Path) -> None:
        self.toolName = "cloc"
        self.command = f"{self.toolName} --by-file --use-sloccount --json {resolvePath(path=path).__str__()}"

        CLOCTool(
            toolName=self.toolName, command=self.command, directoryPath=path
        )
