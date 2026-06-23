from pyspark.sql import functions as F
from pyspark.sql.column import Column

# the four routed error categories are fixed by the silver_deadletter schema
# range and referential failures fold into the nearest category and carry their
# specifics in _error_detail so the DLQ stays queryable on a small enum
ERROR_SCHEMA = "schema_violation"
ERROR_NULL = "null_check"
ERROR_ENUM = "enum_violation"
ERROR_PARSE = "parse_error"

VALID_CHANNELS = ("fednow", "ach")
VALID_OP_TYPES = ("insert", "update", "delete")
CDC_MUTATIONS = ("update", "delete")

# anything above this is operationally impossible for the simulated rail and
# almost always indicates a corrupt or test-injected record
AMOUNT_CEILING = 10_000_000.00


def _parse_failed() -> Column:
    # all typed business fields null while a raw payload exists means the upstream
    # parse produced nothing usable - distinct from a single missing field
    return (
        F.col("_raw_payload").isNotNull()
        & F.col("transaction_id").isNull()
        & F.col("customer_id").isNull()
        & F.col("amount").isNull()
        & F.col("txn_ts").isNull()
    )


def _schema_violation() -> Column:
    # op_type and change_ts are required for downstream cdc ordering, so a row
    # missing them is structurally unusable even if business fields are present
    return (
        F.col("op_type").isNull()
        | F.col("change_ts").isNull()
        | F.col("channel").isNull()
        | (F.col("amount") > AMOUNT_CEILING)
    )


def _null_check() -> Column:
    return (
        F.col("transaction_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("amount").isNull()
        | F.col("txn_ts").isNull()
    )


def _enum_violation() -> Column:
    return (
        ~F.col("channel").isin(*VALID_CHANNELS)
        | ~F.col("op_type").isin(*VALID_OP_TYPES)
    )


def _referential_violation() -> Column:
    # a mutation event with no change_ts cannot be ordered against existing state,
    # and an empty customer_id breaks the dimension join
    return (
        (F.trim(F.col("customer_id")) == "")
        | (F.col("op_type").isin(*CDC_MUTATIONS) & F.col("change_ts").isNull())
    )


def error_type_column() -> Column:
    # first failing check wins - ordered coarsest to finest so the assigned
    # category reflects the most fundamental problem with the row
    return (
        F.when(_parse_failed(), ERROR_PARSE)
        .when(_schema_violation(), ERROR_SCHEMA)
        .when((F.col("amount") <= 0), ERROR_SCHEMA)
        .when(_null_check(), ERROR_NULL)
        .when(_enum_violation(), ERROR_ENUM)
        .when(_referential_violation(), ERROR_NULL)
        .otherwise(F.lit(None))
    )


def error_detail_column() -> Column:
    # human-readable reason for triage - keeps the DLQ self-explanatory without
    # a log round-trip when an analyst opens a failed row
    return (
        F.when(_parse_failed(), F.lit("raw payload present but all typed fields null"))
        .when(F.col("op_type").isNull(), F.lit("op_type missing"))
        .when(F.col("change_ts").isNull() & F.col("op_type").isin(*CDC_MUTATIONS),
              F.concat(F.lit("change_ts missing for op_type "), F.col("op_type")))
        .when(F.col("change_ts").isNull(), F.lit("change_ts missing"))
        .when(F.col("amount") > AMOUNT_CEILING,
              F.concat(F.lit("amount exceeds ceiling: "), F.col("amount").cast("string")))
        .when(F.col("amount") <= 0,
              F.concat(F.lit("amount not positive: "), F.col("amount").cast("string")))
        .when(F.col("transaction_id").isNull(), F.lit("transaction_id null"))
        .when(F.col("customer_id").isNull(), F.lit("customer_id null"))
        .when(F.trim(F.col("customer_id")) == "", F.lit("customer_id empty"))
        .when(F.col("amount").isNull(), F.lit("amount null"))
        .when(F.col("txn_ts").isNull(), F.lit("txn_ts null"))
        .when(~F.col("channel").isin(*VALID_CHANNELS),
              F.concat(F.lit("channel not allowed: "), F.col("channel")))
        .when(~F.col("op_type").isin(*VALID_OP_TYPES),
              F.concat(F.lit("op_type not allowed: "), F.col("op_type")))
        .otherwise(F.lit(None))
    )
