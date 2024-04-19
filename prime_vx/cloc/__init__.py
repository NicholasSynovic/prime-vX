from typing import List

from prime_vx.cloc.cloc import CLOC
from prime_vx.cloc.gocloc import GoCLOC
from prime_vx.cloc.scc import SCC
from prime_vx.cloc.sloccount import SLOCCount

__all__ = [GoCLOC, SCC, CLOC, SLOCCount]

CLOC_KEY_LIST: List[str] = [
    "index",
    "commit_hash",
    "file_count",
    "line_count",
    "blank_line_count",
    "comment_line_count",
    "code_line_count",
    "cloc_tool" "json",
]

CLOC_TOOL_DATA: dict[str, List] = {
    "commit_hash": [],
    "file_count": [],
    "line_count": [],
    "blank_line_count": [],
    "comment_line_count": [],
    "code_line_count": [],
    "tool": [],
    "json": [],
}
