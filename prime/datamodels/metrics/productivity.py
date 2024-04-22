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


class PRODUCTIVITY_MAPPING_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "commit_hash": str,
        "daily_productivity": int,
        "weekly_productivity": int,
        "two_week_productivity": int,
        "monthly_productivity": int,
        "two_month_productivity": int,
        "three_month_productivity": int,
        "six_month_productivity": int,
        "annual_productivity": int,
    }
