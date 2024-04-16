from typedframe import DATE_TIME_DTYPE, TypedDataFrame


class BUS_FACTOR_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "bucket": int,
        "bucket_start": DATE_TIME_DTYPE,
        "bucket_end": DATE_TIME_DTYPE,
        "bus_factor": int,
    }


class BUS_FACTOR_MAPPING_DF_DATAMODEL(TypedDataFrame):
    schema = {
        "commit_hash": str,
        "daily_bus_factor": int,
        "weekly_bus_factor": int,
        "two_week_bus_factor": int,
        "monthly_bus_factor": int,
        "two_month_bus_factor": int,
        "three_month_bus_factor": int,
        "six_month_bus_factor": int,
        "annual_bus_factor": int,
    }
