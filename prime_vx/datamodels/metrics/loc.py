from typedframe import TypedDataFrame


class LOC_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "commitHash": str,
        "loc": int,
        "kloc": float,
        "delta_loc": int,
        "delta_kloc": float,
    }
