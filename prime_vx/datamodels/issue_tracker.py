from typedframe import DATE_TIME_DTYPE, TypedDataFrame


class IT_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "id": int,
        "nodeID": str,
        "state": str,
        "dateOpened": DATE_TIME_DTYPE,
        "dateClosed": DATE_TIME_DTYPE,
        "json": str,
    }
