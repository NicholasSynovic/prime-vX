from typedframe import TypedDataFrame


class CLOC_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "commitHash": str,
        "fileCount": int,
        "lineCount": int,
        "blankLineCount": int,
        "commentLineCount": int,
        "codeLineCount": int,
        "json": str,
    }
