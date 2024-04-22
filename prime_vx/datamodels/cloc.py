from typing import List

from typedframe import TypedDataFrame


class CLOC_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "commit_hash": str,
        "file_count": int,
        "line_count": int,
        "blank_line_count": int,
        "comment_line_count": int,
        "code_line_count": int,
        "json": str,
    }


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
