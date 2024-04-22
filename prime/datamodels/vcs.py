from typedframe import DATE_TIME_DTYPE, TypedDataFrame


class VCS_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "commit_hash": str,
        "tree_hash": str,
        "parent_hash": str,
        "author_name": str,
        "author_email": str,
        "author_date": DATE_TIME_DTYPE,
        "committer_name": str,
        "committer_email": str,
        "committer_date": DATE_TIME_DTYPE,
        "ref_name": str,
        "ref_name_source": str,
        "gpg_signature": str,
        "vcs": str,
        "path": str,
    }
