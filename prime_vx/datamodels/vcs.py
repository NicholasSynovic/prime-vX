from datetime import datetime

from typedframe import DATE_TIME_DTYPE, TypedDataFrame


class VCS_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "commitHash": str,
        "treeHash": str,
        "parentHashes": str,
        "authorName": str,
        "authorEmail": str,
        "authorDate": DATE_TIME_DTYPE,
        "committerName": str,
        "committerEmail": str,
        "committerDate": DATE_TIME_DTYPE,
        "refName": str,
        "refNameSource": str,
        "gpgSignature": str,
        "vcs": str,
        "path": str,
    }
