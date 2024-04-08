from typedframe import TypedDataFrame


class IT_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "id": int,
        "nodeID": str,
        "state": str,
        "dateOpened": float,
        "dateClosed": float,
        "json": str,
    }
