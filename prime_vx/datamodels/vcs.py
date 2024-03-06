from typedframe import TypedDataFrame
from datetime import datetime


class VCS_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "commitHash": str,
        "treeHash": str,
        "parentHashes": str,
        "authorName": str,
        "authorEmail": str,
        "authorDate": datetime,
        "committerName": str,
        "committerEmail": str,
        "committerDate": datetime,
        "refName": str,
        "refNameSource": str,
        "gpgSignature": str,
        "vcs": str,
        "path": str,
    }
