from typedframe import DATE_TIME_DTYPE, TypedDataFrame


class ISSUE_COUNT_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "bucket": int,
        "bucket_start": DATE_TIME_DTYPE,
        "bucket_end": DATE_TIME_DTYPE,
        "issue_count": int,
    }


class ISSUE_COUNT_MAPPING_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "issue_id": int,
        "daily_issue_count": int,
        "weekly_issue_count": int,
        "two_week_issue_count": int,
        "monthly_issue_count": int,
        "two_month_issue_count": int,
        "three_month_issue_count": int,
        "six_month_issue_count": int,
        "annual_issue_count": int,
    }
