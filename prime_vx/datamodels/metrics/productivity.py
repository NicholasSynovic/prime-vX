from typedframe import DATE_TIME_DTYPE, TypedDataFrame


class PRODUCTIVITY_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "bucket": int,
        "bucket_start": DATE_TIME_DTYPE,
        "bucket_end": DATE_TIME_DTYPE,
        "effort_LOC": int,
        "effort_KLOC": float,
        "productivity_LOC": float,
        "productivity_KLOC": float,
    }
