# Handwritten test fixture. This file is not an LLM-generated sample.
from pyspark.sql import functions as F


def transform(df):
    cost = F.trim(F.col("cost"))
    missing_cost = cost.isNull() | F.lower(cost).isin("", "null", "n/a", "tbd")
    numeric = F.regexp_replace(cost, r"^\$", "")
    amount = numeric.try_cast("decimal(18,2)")
    invalid_cost = ~missing_cost & (
        ~numeric.rlike(r"^\+?(?:[0-9]+(?:\.[0-9]{1,2}0*)?|\.[0-9]{1,2}0*)$") | amount.isNull()
    )
    sla = F.trim(F.col("sla_hours"))
    missing_sla = sla.isNull() | F.lower(sla).isin("", "null", "n/a")
    hours = sla.try_cast("int")
    invalid_sla = ~missing_sla & (~sla.rlike(r"^[+-]?[0-9]+$") | hours.isNull() | (hours < 0))
    flags = F.array_compact(F.array(
        F.when(missing_cost, F.lit("cost_missing")),
        F.when(invalid_cost, F.lit("cost_invalid")),
        F.when(missing_sla, F.lit("sla_missing")),
        F.when(invalid_sla, F.lit("sla_invalid")),
        F.when(~missing_sla & ~invalid_sla & hours.isin(0, 999), F.lit("sla_requires_confirmation")),
    ))
    return df.select(
        *[F.col(name) for name in df.columns if name not in {"cost", "sla_hours", "quality_flags"}],
        F.when(~missing_cost & ~invalid_cost, amount).alias("cost"),
        F.when(~missing_sla & ~invalid_sla, hours).alias("sla_hours"),
        flags.alias("quality_flags"),
    )
