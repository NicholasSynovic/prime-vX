from typedframe import DATE_TIME_DTYPE, TypedDataFrame


class DEVELOPER_COUNT_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "bucket": int,
        "bucket_start": DATE_TIME_DTYPE,
        "bucket_end": DATE_TIME_DTYPE,
        "developer_count": int,
    }


class DEVELOPER_COUNT_MAPPING_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "commit_hash": str,
        "daily_developer_count": int,
        "weekly_developer_count": int,
        "two_week_developer_count": int,
        "monthly_developer_count": int,
        "two_month_developer_count": int,
        "three_month_developer_count": int,
        "six_month_developer_count": int,
        "annual_developer_count": int,
    }
