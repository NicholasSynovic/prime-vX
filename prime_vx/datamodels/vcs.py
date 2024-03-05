from typedframe import TypedDataFrame


class VCS_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "commitHash": str,
        "treeHash": str,
        "parentHashes": str,
        "authorName": str,
        "authorEmail": str,
        "authorDate": float,
        "committerName": str,
        "committerEmail": str,
        "committerDate": float,
        "refName": str,
        "refNameSource": str,
        "gpgSignature": str,
        "vcs": str,
        "path": str,
    }
