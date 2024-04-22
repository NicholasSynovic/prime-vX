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

CLOC_TOOL_JSON: dict[str, List[str | int]] = {
    "blank_line_count": [],
    "code_line_count": [],
    "comment_line_count": [],
    "files": [],
    "languages": [],
    "line_count": [],
}

JSON_SCHEMA: dict = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": [
        "blank_line_count",
        "code_line_count",
        "comment_line_count",
        "files",
        "languages",
        "line_count",
    ],
    "properties": {
        "blank_line_count": {
            "type": "array",
            "default": [],
            "items": {"type": "integer", "default": 0},
        },
        "code_line_count": {
            "type": "array",
            "default": [],
            "items": {"type": "integer", "default": 0},
        },
        "comment_line_count": {
            "type": "array",
            "default": [],
            "items": {"type": "integer", "default": 0},
        },
        "files": {
            "type": "array",
            "default": [],
            "items": {"type": "string", "default": "", "pattern": "^.*$"},
        },
        "languages": {
            "type": "array",
            "default": [],
            "items": {"type": "string", "default": "", "pattern": "^.*$"},
        },
        "line_count": {
            "type": "array",
            "default": [],
            "items": {"type": "integer", "default": 0},
        },
    },
}
