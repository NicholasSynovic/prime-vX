from typedframe import DATE_TIME_DTYPE, TypedDataFrame


class ISSUE_SPOILAGE_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "bucket": int,
        "bucket_start": DATE_TIME_DTYPE,
        "bucket_end": DATE_TIME_DTYPE,
        "spoiled_issues": int,
    }
