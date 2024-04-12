from typedframe import DATE_TIME_DTYPE, TypedDataFrame


class IT_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "id": int,
        "node_id": str,
        "state": str,
        "date_opened": DATE_TIME_DTYPE,
        "date_closed": DATE_TIME_DTYPE,
        "url": str,
        "json": str,
    }
