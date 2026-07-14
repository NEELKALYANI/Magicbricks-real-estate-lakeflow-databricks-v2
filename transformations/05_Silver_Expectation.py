# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog", "adb_real_estate_mb")
SILVER_CLEANED = f"{CATALOG}.silver.silver_cleaned"
SILVER_QUARANTINE = f"{CATALOG}.silver.silver_quarantine"
SILVER_VALIDATED = f"{CATALOG}.silver.silver_validated"


def required(rule):
    return F.coalesce(rule, F.lit(False))


def optional(rule):
    return F.coalesce(rule, F.lit(True))


RULES = [
    ("listing_id_exists", required(F.col("listing_id").isNotNull())),
    ("listing_id_positive", required(F.col("listing_id") > 0)),
    ("price_reasonable", required(F.col("price").between(500000, 1000000000))),
    ("covered_area_valid", optional((F.col("covered_area_sqft").isNull()) | (F.col("covered_area_sqft") >= 100))),
    ("bedrooms_valid", required(F.col("bedrooms").between(1, 10))),
    ("bathrooms_valid", optional((F.col("bathrooms").isNull()) | (F.col("bathrooms").between(1, 10)))),
    ("floor_within_building", optional(
        F.when(
            F.col("floor_no").isNull() | F.col("total_floors").isNull(),
            True
        ).otherwise(F.col("floor_no") <= F.col("total_floors"))
    )),
    ("title_length", required(F.length(F.trim(F.col("property_title"))) >= 15)),
    ("url_valid", required(F.col("url").rlike(r"^(http|https|[0-9].*)"))),
    ("coordinates_valid_if_present", optional(
        F.col("latitude").isNull() & F.col("longitude").isNull()
        | (F.col("latitude").between(-90, 90) & F.col("longitude").between(-180, 180))
    )),
    ("has_images", required(F.col("image_count") > 0)),
]

def apply_quality(df):
    failed=[F.when(~expr,F.lit(name)) for name,expr in RULES]
    return (df
      .withColumn("failed_expectations",F.filter(F.array(*failed), lambda x: x.isNotNull()))
      .withColumn("failed_expectation_count",F.size("failed_expectations"))
      .withColumn("is_valid",F.col("failed_expectation_count")==0)
    )

dp.create_streaming_table(
    name=SILVER_QUARANTINE,
    comment="Business rule failures",
    table_properties={"quality":"silver","layer":"quarantine"}
)

@dp.temporary_view(name="silver_quality_evaluated")
def silver_quality_evaluated():
    return apply_quality(spark.readStream.table(SILVER_CLEANED))


@dp.append_flow(target=SILVER_QUARANTINE, name="silver_quarantine")
def quarantine():
    return (spark.readStream.table("silver_quality_evaluated")
        .filter(F.col("failed_expectation_count")>0)
        .withColumn("quarantine_timestamp",F.current_timestamp()))

dp.create_streaming_table(
    name=SILVER_VALIDATED,
    comment="Validated business records",
    table_properties={"quality":"silver","layer":"validated"}
)

@dp.append_flow(target=SILVER_VALIDATED, name="silver_validated")
def validated():
    return (spark.readStream.table("silver_quality_evaluated")
        .filter(F.col("failed_expectation_count")==0)
        .drop("is_valid")
        .withColumn("validated_timestamp",F.current_timestamp()))
