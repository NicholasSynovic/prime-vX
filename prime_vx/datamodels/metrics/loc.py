from typedframe import TypedDataFrame


class LOC_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "commit_hash": str,
        "loc": int,
        "kloc": float,
        "delta_loc": int,
        "delta_kloc": float,
    }
