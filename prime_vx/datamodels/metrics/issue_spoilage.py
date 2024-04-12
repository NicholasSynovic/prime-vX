from typedframe import DATE_TIME_DTYPE, TypedDataFrame


class ISSUE_SPOILAGE_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "bucket": int,
        "bucket_start": DATE_TIME_DTYPE,
        "bucket_end": DATE_TIME_DTYPE,
        "spoiled_issue_spoilage": int,
    }


class ISSUE_SPOLAGE_MAPPING_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "issue_id": int,
        "daily_issue_spoilage": int,
        "weekly_issue_spoilage": int,
        "two_week_issue_spoilage": int,
        "monthly_issue_spoilage": int,
        "two_month_issue_spoilage": int,
        "three_month_issue_spoilage": int,
        "six_month_issue_spoilage": int,
        "annual_issue_spoilage": int,
    }
