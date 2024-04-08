from typedframe import TypedDataFrame


class IT_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "id": int,
        "node_id": str,
        "state": str,
        "date_opened": float,
        "date_closed": float,
        "url": str,
        "json": str,
    }
