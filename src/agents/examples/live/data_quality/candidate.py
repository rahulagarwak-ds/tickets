from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType


def transform(df):
    """Normalize cost and SLA values and append their quality flags."""
    cost_raw = F.trim(F.col("cost"))
    cost_text = F.trim(F.regexp_replace(cost_raw, r"^[$] *", ""))
    sla_text = F.trim(F.col("sla_hours"))

    cost_missing = F.coalesce(
        cost_raw.isNull() | F.upper(cost_raw).isin("", "NULL", "N/A", "TBD"),
        F.lit(False),
    )
    sla_missing = F.coalesce(
        sla_text.isNull() | F.upper(sla_text).isin("", "NULL", "N/A"),
        F.lit(False),
    )

    cost_syntax = cost_text.rlike(r"^[+-]?(?:[0-9]+(?:[.][0-9]*)?|[.][0-9]+)$")
    cost_num = cost_text.try_cast("decimal(18,2)")
    fraction = F.regexp_extract(cost_text, r"[.]([0-9]*)$", 1)
    excess_nonzero = F.substring(fraction, 3, 100000).rlike(r"[1-9]")
    # Inspect original digits so values such as -0.001 are not rounded to zero.
    negative_nonzero = (
        cost_text.rlike(r"^-")
        & F.regexp_replace(cost_text, r"[^0-9]", "").rlike(r"[1-9]")
    )
    cost_invalid = F.coalesce(
        (~cost_missing)
        & (
            (~cost_syntax)
            | cost_num.isNull()
            | negative_nonzero
            | excess_nonzero
        ),
        F.lit(False),
    )

    sla_syntax = sla_text.rlike(r"^[+-]?[0-9]+$")
    sla_num = sla_text.try_cast("bigint")
    sla_invalid = F.coalesce(
        (~sla_missing)
        & (
            (~sla_syntax)
            | sla_num.isNull()
            | (sla_num < 0)
            | (sla_num > 2147483647)
        ),
        F.lit(False),
    )
    sla_confirm = F.coalesce(
        (~sla_missing) & (~sla_invalid) & sla_num.isin(0, 999),
        F.lit(False),
    )

    empty_flags = F.array().cast(ArrayType(StringType()))

    def flag(condition, name):
        return F.when(condition, F.array(F.lit(name))).otherwise(empty_flags)

    additions = F.concat(
        flag(cost_missing, "cost_missing"),
        flag(cost_invalid, "cost_invalid"),
        flag(sla_missing, "sla_missing"),
        flag(sla_invalid, "sla_invalid"),
        flag(sla_confirm, "sla_requires_confirmation"),
    )
    prior = (
        F.coalesce(F.col("quality_flags"), empty_flags)
        if "quality_flags" in df.columns
        else empty_flags
    )
    replacements = {
        "cost": F.when(
            cost_missing | cost_invalid, F.lit(None).cast("decimal(18,2)")
        ).otherwise(cost_num).alias("cost"),
        "sla_hours": F.when(
            sla_missing | sla_invalid, F.lit(None).cast("int")
        ).otherwise(sla_num.cast("int")).alias("sla_hours"),
        "quality_flags": F.concat(prior, additions).alias("quality_flags"),
    }
    projection = [replacements.get(name, F.col(name)) for name in df.columns]
    if "quality_flags" not in df.columns:
        projection.append(replacements["quality_flags"])
    return df.select(*projection)