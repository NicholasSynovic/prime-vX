from typedframe import DATE_TIME_DTYPE, TypedDataFrame


class ISSUE_DENSITY_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "bucket": int,
        "bucket_start": DATE_TIME_DTYPE,
        "bucket_end": DATE_TIME_DTYPE,
        "issue_density": int,
    }


class ISSUE_DENSITY_MAPPING_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "issue_id": int,
        "daily_issue_density": int,
        "weekly_issue_density": int,
        "two_week_issue_density": int,
        "monthly_issue_density": int,
        "two_month_issue_density": int,
        "three_month_issue_density": int,
        "six_month_issue_density": int,
        "annual_issue_density": int,
    }
