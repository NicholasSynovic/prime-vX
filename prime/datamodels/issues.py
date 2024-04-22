from typedframe import DATE_TIME_DTYPE, TypedDataFrame


class ISSUE_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "id": str,
        "opened": DATE_TIME_DTYPE,
        "closed": DATE_TIME_DTYPE,
        "json": dict,
    }
