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
