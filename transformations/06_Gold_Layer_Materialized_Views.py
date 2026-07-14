# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

# Databricks Lakeflow / DLT Gold Layer Materialized Views
# =======================================================
# Reads : adb_real_estate_mb.silver.silver_validated
# Writes: adb_real_estate_mb.gold.*
#
# Business grain:
# One row in Silver represents one validated Magicbricks listing record.
#
# Gold design:
# 1. gold_listing_master_current
#    - One latest/current record per listing_id.
#    - Adds business-facing derived columns used by all downstream views.
#
# 2. Dashboard-ready materialized views
#    - Current-state summaries for city, locality, segment, project,
#      freshness, and quality coverage dashboards.
#
# Notes:
# - This project is a one-time ingestion, so current-state Gold views are
#   preferred over daily historical snapshot tables.
# - snapshot_date is still included to make dashboards easy to label and to
#   allow the same design to evolve into daily snapshots later.
# - Silver is intentionally left unchanged. Gold handles deduplication,
#   coordinate usability, dashboard categorization, benchmark flags,
#   benchmark confidence, and quality scoring.


CATALOG = spark.conf.get("catalog", "adb_real_estate_mb")

SILVER_VALIDATED = f"{CATALOG}.silver.silver_validated"

GOLD_LISTING_MASTER = f"{CATALOG}.gold.gold_listing_master_current"
GOLD_CITY_SUMMARY = f"{CATALOG}.gold.gold_city_summary"
GOLD_LOCALITY_SUMMARY = f"{CATALOG}.gold.gold_locality_summary"
GOLD_MARKET_INVENTORY = f"{CATALOG}.gold.gold_market_inventory"
GOLD_PRICE_BENCHMARK = f"{CATALOG}.gold.gold_price_benchmark"
GOLD_PROJECT_SUMMARY = f"{CATALOG}.gold.gold_project_summary"
GOLD_FRESHNESS_SUMMARY = f"{CATALOG}.gold.gold_freshness_summary"
GOLD_QUALITY_COVERAGE = f"{CATALOG}.gold.gold_quality_coverage"


# ---------------------------------------------------------------------
# Helper expressions
# ---------------------------------------------------------------------

def clean_text(col_name):
    return F.trim(F.col(col_name))


def has_text(col_name):
    return F.col(col_name).isNotNull() & (F.length(F.trim(F.col(col_name))) > 0)


def count_when(condition):
    return F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0)))


def share_when(condition):
    return F.round(count_when(condition) / F.count(F.lit(1)), 4)


def median_expr(col_name):
    return F.expr(f"percentile_approx({col_name}, 0.5, 10000)")


def percentile_expr(col_name, percentile):
    return F.expr(f"percentile_approx({col_name}, {percentile}, 10000)")


def safe_avg(col_name):
    return F.round(F.avg(F.col(col_name)), 2)


def safe_median(col_name):
    return F.round(median_expr(col_name), 2)


def safe_pct(condition):
    return F.round(100.0 * count_when(condition) / F.count(F.lit(1)), 2)


def unknown_if_blank(col_name, fallback):
    return (
        F.when(
            F.col(col_name).isNull() | (F.length(F.trim(F.col(col_name))) == 0),
            F.lit(fallback)
        ).otherwise(F.trim(F.col(col_name)))
    )


def benchmark_confidence_expr():
    return (
        F.when(F.col("listing_count") >= 20, F.lit("High"))
         .when(F.col("listing_count") >= 5, F.lit("Medium"))
         .otherwise(F.lit("Low"))
    )


def is_dashboard_benchmark_expr():
    return F.col("listing_count") >= 5


def gold_master_df():
    df = spark.read.table(SILVER_VALIDATED)

    # -----------------------------------------------------------------
    # Parse operational timestamps for deterministic current-state dedupe.
    # modified_ts is the preferred business freshness signal, but some rows
    # may carry a 1900 fallback value, so ingestion/validation timestamps are
    # used as tie-breakers.
    # -----------------------------------------------------------------
    df = (
        df
        .withColumn("ingestion_ts", F.to_timestamp("ingestion_time"))
        .withColumn("validated_ts", F.to_timestamp("validated_timestamp"))
        .withColumn("modified_ts_clean", F.col("modified_ts"))
    )

    dedupe_window = (
        Window
        .partitionBy("listing_id")
        .orderBy(
            F.col("modified_ts_clean").desc_nulls_last(),
            F.col("ingestion_ts").desc_nulls_last(),
            F.col("validated_ts").desc_nulls_last(),
            F.col("enc_id").desc_nulls_last()
        )
    )

    df = (
        df
        .withColumn("listing_current_rank", F.row_number().over(dedupe_window))
        .filter(F.col("listing_current_rank") == 1)
        .drop("listing_current_rank")
    )

    # -----------------------------------------------------------------
    # Derived business columns.
    # -----------------------------------------------------------------
    price_per_sqft = F.when(
        F.col("covered_area_sqft").isNotNull() & (F.col("covered_area_sqft") > 0),
        F.col("price") / F.col("covered_area_sqft")
    ).otherwise(F.col("sqft_price"))

    carpet_area_sqft = F.when(
        F.lower(F.col("carpet_area_unit")).isin("sq-ft", "sqft", "sq ft", "square feet"),
        F.col("carpet_area_raw").cast("double")
    )

    has_coordinates = (
        F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
        & F.col("latitude").between(-90, 90)
        & F.col("longitude").between(-180, 180)
        & ~((F.col("latitude") == 0) & (F.col("longitude") == 0))
    )

    possession_month = F.to_date(
        F.concat(F.lit("01 "), F.col("possession_status")),
        "dd MMM ''yy"
    )

    df = (
        df
        .withColumn("snapshot_date", F.current_date())
        .withColumn("city_name", unknown_if_blank("city_name", "Unknown City"))
        .withColumn("locality_name", unknown_if_blank("locality_name", "Unknown Locality"))
        .withColumn("project_name", clean_text("project_name"))
        .withColumn("dev_name", clean_text("dev_name"))
        .withColumn("property_type", clean_text("property_type"))
        .withColumn("user_type", clean_text("user_type"))
        .withColumn("price_per_sqft", F.round(price_per_sqft, 2))
        .withColumn("carpet_area_sqft", carpet_area_sqft)
        .withColumn(
            "carpet_price_per_sqft",
            F.when(
                F.col("carpet_area_sqft").isNotNull() & (F.col("carpet_area_sqft") > 0),
                F.round(F.col("price") / F.col("carpet_area_sqft"), 2)
            )
        )
        .withColumn("has_coordinates", has_coordinates)
        .withColumn("has_project", has_text("project_name"))
        .withColumn("has_developer", has_text("dev_name"))
        .withColumn("has_landmark", has_text("landmark"))
        .withColumn("has_description", has_text("auto_desc") | has_text("seo_desc") | has_text("detail_desc"))
        .withColumn("has_images", F.col("image_count") > 0)
        .withColumn("amenity_count", F.when(F.col("amenities").isNotNull(), F.size("amenities")).otherwise(F.lit(0)))
        .withColumn("lux_amenity_count", F.when(F.col("lux_amenities").isNotNull(), F.size("lux_amenities")).otherwise(F.lit(0)))
        .withColumn("is_owner_listing", F.lower(F.col("user_type")) == F.lit("owner"))
        .withColumn("is_broker_listing", F.lower(F.col("user_type")).isin("agent", "broker"))
        .withColumn("is_builder_listing", F.lower(F.col("user_type")) == F.lit("builder"))
        .withColumn(
            "possession_month",
            F.when(possession_month.isNotNull(), possession_month)
        )
        .withColumn(
            "possession_category",
            F.when(F.lower(F.col("possession_status")).isin("ready to move", "ready-to-move"), F.lit("Ready to Move"))
             .when(F.lower(F.col("possession_status")).isin("under construction", "under-construction"), F.lit("Under Construction"))
             .when(F.col("possession_month").isNotNull(), F.lit("Under Construction"))
             .otherwise(F.lit("Unknown"))
        )
        .withColumn("posted_date", F.to_date("posted_date_raw"))
        .withColumn("available_from_date", F.coalesce(F.col("available_from"), F.col("possession_month")))
        .withColumn("listing_age_days", F.datediff(F.current_date(), F.col("posted_date")))
        .withColumn(
            "days_since_modified",
            F.datediff(
                F.current_date(),
                F.to_date(F.coalesce("modified_ts_clean", "source_file_modification_time")),
            ),
        )
        .withColumn("is_stale_30d", F.col("listing_age_days") > 30)
        .withColumn("is_stale_60d", F.col("listing_age_days") > 60)
        .withColumn("is_stale_90d", F.col("listing_age_days") > 90)
        .withColumn(
            "price_bucket",
            F.when(F.col("price") < 5000000, F.lit("<50L"))
             .when(F.col("price") < 10000000, F.lit("50L-1Cr"))
             .when(F.col("price") < 20000000, F.lit("1Cr-2Cr"))
             .when(F.col("price") < 50000000, F.lit("2Cr-5Cr"))
             .when(F.col("price") < 100000000, F.lit("5Cr-10Cr"))
             .otherwise(F.lit("10Cr+"))
        )
        .withColumn(
            "covered_area_bucket",
            F.when(F.col("covered_area_sqft").isNull(), F.lit("Unknown"))
             .when(F.col("covered_area_sqft") < 500, F.lit("<500 sqft"))
             .when(F.col("covered_area_sqft") < 1000, F.lit("500-999 sqft"))
             .when(F.col("covered_area_sqft") < 1500, F.lit("1000-1499 sqft"))
             .when(F.col("covered_area_sqft") < 2500, F.lit("1500-2499 sqft"))
             .when(F.col("covered_area_sqft") < 5000, F.lit("2500-4999 sqft"))
             .otherwise(F.lit("5000+ sqft"))
        )
        .withColumn(
            "bedroom_bucket",
            F.when(F.col("bedrooms").isNull(), F.lit("Unknown"))
             .when(F.col("bedrooms") <= 4, F.concat(F.col("bedrooms").cast("string"), F.lit(" BHK")))
             .otherwise(F.lit("5+ BHK"))
        )
        .withColumn(
            "is_ppsf_outlier",
            F.col("price_per_sqft").isNull()
            | (F.col("price_per_sqft") > 100000)
            | (F.col("price_per_sqft") < 500)
            | (F.col("covered_area_sqft") > 10000)
        )
        .withColumn(
            "benchmark_price_per_sqft",
            F.when(~F.col("is_ppsf_outlier"), F.col("price_per_sqft"))
        )
    )

    locality_window = Window.partitionBy("city_name", "locality_name")
    locality_segment_window = Window.partitionBy("city_name", "locality_name", "property_type", "bedrooms")

    df = (
        df
        .withColumn("locality_median_price", F.round(F.percentile_approx("price", 0.5, 10000).over(locality_window), 2))
        .withColumn("locality_median_price_per_sqft", F.round(F.percentile_approx("benchmark_price_per_sqft", 0.5, 10000).over(locality_window), 2))
        .withColumn("segment_median_price", F.round(F.percentile_approx("price", 0.5, 10000).over(locality_segment_window), 2))
        .withColumn("segment_median_price_per_sqft", F.round(F.percentile_approx("benchmark_price_per_sqft", 0.5, 10000).over(locality_segment_window), 2))
        .withColumn(
            "price_vs_locality_median_pct",
            F.when(
                F.col("locality_median_price") > 0,
                F.round(100.0 * (F.col("price") - F.col("locality_median_price")) / F.col("locality_median_price"), 2)
            )
        )
        .withColumn(
            "ppsf_vs_locality_median_pct",
            F.when(
                F.col("locality_median_price_per_sqft") > 0,
                F.round(100.0 * (F.col("price_per_sqft") - F.col("locality_median_price_per_sqft")) / F.col("locality_median_price_per_sqft"), 2)
            )
        )
    )

    return df


@dp.materialized_view(
    name=GOLD_LISTING_MASTER,
    comment="Current-state, deduplicated, business-ready listing master for Magicbricks analytics.",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "mart": "listing_master",
        "grain": "one_row_per_current_listing_id"
    }
)
def gold_listing_master_current():
    return gold_master_df()


@dp.materialized_view(
    name=GOLD_CITY_SUMMARY,
    comment="Current city-level real-estate listing inventory and pricing summary.",
    table_properties={"quality": "gold", "layer": "gold", "mart": "city_summary"}
)
def gold_city_summary():
    df = spark.read.table(GOLD_LISTING_MASTER)

    return (
        df
        .groupBy("snapshot_date", "city_name")
        .agg(
            F.count("*").alias("listing_count"),
            F.countDistinct("locality_name").alias("locality_count"),
            F.countDistinct("project_name").alias("distinct_project_count"),
            F.countDistinct("dev_id").alias("distinct_developer_count"),
            F.countDistinct("owner_id").alias("distinct_seller_count"),
            safe_avg("price").alias("avg_price"),
            safe_median("price").alias("median_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            safe_avg("benchmark_price_per_sqft").alias("avg_price_per_sqft"),
            safe_median("benchmark_price_per_sqft").alias("median_price_per_sqft"),
            safe_avg("covered_area_sqft").alias("avg_covered_area_sqft"),
            safe_median("covered_area_sqft").alias("median_covered_area_sqft"),
            count_when(F.col("property_type") == "Apartment").alias("apartment_listing_count"),
            count_when(F.col("property_type") == "Villa").alias("villa_listing_count"),
            count_when(F.col("property_type").isin("Residential House", "Builder Floor Apartment")).alias("house_or_builder_floor_count"),
            count_when(F.col("possession_category") == "Ready to Move").alias("ready_to_move_count"),
            count_when(F.col("possession_category") == "Under Construction").alias("under_construction_count"),
            count_when(F.col("is_owner_listing")).alias("owner_listing_count"),
            count_when(F.col("is_broker_listing")).alias("broker_listing_count"),
            count_when(F.col("is_builder_listing")).alias("builder_listing_count"),
            count_when(F.col("furnished_status") == "Furnished").alias("furnished_listing_count"),
            count_when(F.col("furnished_status") == "Semi-furnished").alias("semi_furnished_listing_count"),
            count_when(F.col("furnished_status") == "Unfurnished").alias("unfurnished_listing_count"),
            safe_pct(F.col("has_coordinates")).alias("pct_with_coordinates"),
            safe_pct(F.col("has_project")).alias("pct_with_project_name")
        )
    )


@dp.materialized_view(
    name=GOLD_LOCALITY_SUMMARY,
    comment="Current locality-level micro-market inventory, pricing, and seller-mix summary.",
    table_properties={"quality": "gold", "layer": "gold", "mart": "locality_summary"}
)
def gold_locality_summary():
    df = spark.read.table(GOLD_LISTING_MASTER)

    return (
        df
        .groupBy("snapshot_date", "city_name", "locality_name")
        .agg(
            F.count("*").alias("listing_count"),
            F.countDistinct("project_name").alias("distinct_project_count"),
            F.countDistinct("dev_id").alias("distinct_developer_count"),
            F.countDistinct("owner_id").alias("distinct_seller_count"),
            safe_avg("price").alias("avg_price"),
            safe_median("price").alias("median_price"),
            F.round(percentile_expr("price", 0.25), 2).alias("p25_price"),
            F.round(percentile_expr("price", 0.75), 2).alias("p75_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            safe_avg("benchmark_price_per_sqft").alias("avg_price_per_sqft"),
            safe_median("benchmark_price_per_sqft").alias("median_price_per_sqft"),
            safe_avg("covered_area_sqft").alias("avg_covered_area_sqft"),
            safe_median("covered_area_sqft").alias("median_covered_area_sqft"),
            share_when(F.col("is_owner_listing")).alias("owner_share"),
            share_when(F.col("is_broker_listing")).alias("broker_share"),
            share_when(F.col("is_builder_listing")).alias("builder_share"),
            share_when(F.col("possession_category") == "Ready to Move").alias("ready_to_move_share"),
            share_when(F.col("possession_category") == "Under Construction").alias("under_construction_share"),
            share_when(F.col("property_type") == "Apartment").alias("apartment_share"),
            share_when(F.col("property_type") == "Villa").alias("villa_share"),
            share_when(F.col("furnished_status") == "Furnished").alias("furnished_share"),
            safe_pct(F.col("has_coordinates")).alias("pct_with_coordinates"),
            safe_pct(F.col("has_project")).alias("pct_with_project_name")
        )
    )


@dp.materialized_view(
    name=GOLD_MARKET_INVENTORY,
    comment="Current segmented market inventory by city, locality, property type, and bedroom configuration.",
    table_properties={"quality": "gold", "layer": "gold", "mart": "market_inventory"}
)
def gold_market_inventory():
    df = spark.read.table(GOLD_LISTING_MASTER)

    return (
        df
        .groupBy(
            "snapshot_date",
            "city_name",
            "locality_name",
            "property_type",
            "bedrooms",
            "bedroom_bucket"
        )
        .agg(
            F.count("*").alias("listing_count"),
            F.countDistinct("owner_id").alias("distinct_seller_count"),
            F.countDistinct("project_name").alias("distinct_project_count"),
            safe_avg("price").alias("avg_price"),
            safe_median("price").alias("median_price"),
            safe_avg("benchmark_price_per_sqft").alias("avg_price_per_sqft"),
            safe_median("benchmark_price_per_sqft").alias("median_price_per_sqft"),
            safe_avg("covered_area_sqft").alias("avg_covered_area_sqft"),
            safe_median("covered_area_sqft").alias("median_covered_area_sqft"),
            count_when(F.col("possession_category") == "Ready to Move").alias("ready_to_move_count"),
            count_when(F.col("possession_category") == "Under Construction").alias("under_construction_count"),
            count_when(F.col("is_owner_listing")).alias("owner_listing_count"),
            count_when(F.col("is_broker_listing")).alias("broker_listing_count"),
            count_when(F.col("is_builder_listing")).alias("builder_listing_count")
        )
        .withColumn("benchmark_confidence", benchmark_confidence_expr())
        .withColumn("is_dashboard_benchmark", is_dashboard_benchmark_expr())
    )


@dp.materialized_view(
    name=GOLD_PRICE_BENCHMARK,
    comment="Current locality and segment-level asking-price and price-per-sqft benchmarks.",
    table_properties={"quality": "gold", "layer": "gold", "mart": "price_benchmark"}
)
def gold_price_benchmark():
    df = spark.read.table(GOLD_LISTING_MASTER)

    segment = (
        df
        .groupBy("snapshot_date", "city_name", "locality_name", "property_type", "bedrooms", "bedroom_bucket")
        .agg(
            F.count("*").alias("listing_count"),
            safe_avg("price").alias("avg_price"),
            safe_median("price").alias("median_price"),
            F.round(F.stddev("price"), 2).alias("stddev_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            safe_avg("benchmark_price_per_sqft").alias("avg_price_per_sqft"),
            safe_median("benchmark_price_per_sqft").alias("median_price_per_sqft"),
            F.round(percentile_expr("benchmark_price_per_sqft", 0.10), 2).alias("p10_price_per_sqft"),
            F.round(percentile_expr("benchmark_price_per_sqft", 0.25), 2).alias("p25_price_per_sqft"),
            F.round(percentile_expr("benchmark_price_per_sqft", 0.75), 2).alias("p75_price_per_sqft"),
            F.round(percentile_expr("benchmark_price_per_sqft", 0.90), 2).alias("p90_price_per_sqft")
        )
    )

    city_segment = (
        df
        .groupBy("snapshot_date", "city_name", "property_type", "bedrooms")
        .agg(
            safe_median("benchmark_price_per_sqft").alias("city_median_ppsf_for_same_segment")
        )
    )

    return (
        segment.alias("s")
        .join(
            city_segment.alias("c"),
            on=["snapshot_date", "city_name", "property_type", "bedrooms"],
            how="left"
        )
        .withColumn(
            "locality_ppsf_premium_vs_city_pct",
            F.when(
                F.col("city_median_ppsf_for_same_segment") > 0,
                F.round(
                    100.0
                    * (F.col("median_price_per_sqft") - F.col("city_median_ppsf_for_same_segment"))
                    / F.col("city_median_ppsf_for_same_segment"),
                    2
                )
            )
        )
        .withColumn(
            "price_dispersion_index",
            F.when(
                F.col("median_price_per_sqft") > 0,
                F.round((F.col("p75_price_per_sqft") - F.col("p25_price_per_sqft")) / F.col("median_price_per_sqft"), 4)
            )
        )
        .withColumn("benchmark_confidence", benchmark_confidence_expr())
        .withColumn("is_dashboard_benchmark", is_dashboard_benchmark_expr())
    )


@dp.materialized_view(
    name=GOLD_PROJECT_SUMMARY,
    comment="Current project and society-level inventory and pricing intelligence.",
    table_properties={"quality": "gold", "layer": "gold", "mart": "project_summary"}
)
def gold_project_summary():
    df = spark.read.table(GOLD_LISTING_MASTER).filter(has_text("project_name"))

    return (
        df
        .groupBy("snapshot_date", "city_name", "locality_name", "project_name", "dev_id", "dev_name")
        .agg(
            F.count("*").alias("listing_count"),
            F.countDistinct("owner_id").alias("distinct_seller_count"),
            F.countDistinct("property_type").alias("property_type_count"),
            F.countDistinct("bedrooms").alias("bedroom_config_count"),
            safe_avg("price").alias("avg_price"),
            safe_median("price").alias("median_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            safe_avg("benchmark_price_per_sqft").alias("avg_price_per_sqft"),
            safe_median("benchmark_price_per_sqft").alias("median_price_per_sqft"),
            safe_avg("covered_area_sqft").alias("avg_covered_area_sqft"),
            safe_median("covered_area_sqft").alias("median_covered_area_sqft"),
            count_when(F.col("possession_category") == "Ready to Move").alias("ready_to_move_count"),
            count_when(F.col("possession_category") == "Under Construction").alias("under_construction_count"),
            count_when(F.col("is_owner_listing")).alias("owner_listing_count"),
            count_when(F.col("is_broker_listing")).alias("broker_listing_count"),
            count_when(F.col("is_builder_listing")).alias("builder_listing_count"),
            safe_pct(F.col("has_coordinates")).alias("pct_with_coordinates")
        )
    )


@dp.materialized_view(
    name=GOLD_FRESHNESS_SUMMARY,
    comment="Current freshness and staleness monitoring by city and locality.",
    table_properties={"quality": "gold", "layer": "gold", "mart": "freshness_summary"}
)
def gold_freshness_summary():
    df = spark.read.table(GOLD_LISTING_MASTER)

    return (
        df
        .groupBy("snapshot_date", "city_name", "locality_name")
        .agg(
            F.count("*").alias("listing_count"),
            count_when(F.col("listing_age_days") <= 7).alias("new_last_7d_count"),
            count_when(F.col("listing_age_days") <= 30).alias("new_last_30d_count"),
            count_when(F.col("days_since_modified") <= 7).alias("modified_last_7d_count"),
            count_when(F.col("days_since_modified") <= 30).alias("modified_last_30d_count"),
            count_when(F.col("is_stale_30d")).alias("stale_30d_count"),
            count_when(F.col("is_stale_60d")).alias("stale_60d_count"),
            count_when(F.col("is_stale_90d")).alias("stale_90d_count"),
            safe_avg("listing_age_days").alias("avg_listing_age_days"),
            safe_median("listing_age_days").alias("median_listing_age_days"),
            safe_median("days_since_modified").alias("median_days_since_modified"),
            share_when(F.col("listing_age_days") <= 30).alias("new_30d_share"),
            share_when(F.col("is_stale_60d")).alias("stale_60d_share"),
            share_when(F.col("is_stale_90d")).alias("stale_90d_share")
        )
    )


@dp.materialized_view(
    name=GOLD_QUALITY_COVERAGE,
    comment="Current metadata completeness and analytical coverage by city and locality.",
    table_properties={"quality": "gold", "layer": "gold", "mart": "quality_coverage"}
)
def gold_quality_coverage():
    df = spark.read.table(GOLD_LISTING_MASTER)

    return (
        df
        .groupBy("snapshot_date", "city_name", "locality_name")
        .agg(
            F.count("*").alias("listing_count"),
            safe_pct(F.col("has_coordinates")).alias("pct_with_coordinates"),
            safe_pct(F.col("has_images")).alias("pct_with_images"),
            safe_pct(F.col("has_project")).alias("pct_with_project_name"),
            safe_pct(F.col("has_developer")).alias("pct_with_developer_name"),
            safe_pct(F.col("has_landmark")).alias("pct_with_landmark"),
            safe_pct(F.col("has_description")).alias("pct_with_description"),
            safe_pct(F.col("amenity_count") > 0).alias("pct_with_amenities"),
            safe_pct(F.col("lux_amenity_count") > 0).alias("pct_with_lux_amenities"),
            safe_pct(F.col("floor_no").isNotNull() & F.col("total_floors").isNotNull()).alias("pct_with_floor_info"),
            safe_pct(F.col("bathrooms").isNotNull()).alias("pct_with_bathrooms"),
            safe_pct(F.col("covered_area_sqft").isNotNull()).alias("pct_with_covered_area"),
            safe_pct(F.col("carpet_area_sqft").isNotNull()).alias("pct_with_carpet_area"),
            safe_pct(has_text("owner_name")).alias("pct_with_owner_name"),
            safe_pct(has_text("company_name")).alias("pct_with_company_name"),
            safe_pct(~F.col("is_ppsf_outlier")).alias("pct_usable_for_ppsf_benchmark")
        )
        .withColumn(
            "quality_score",
            F.round(
                (
                    F.col("pct_with_coordinates")
                    + F.col("pct_with_images")
                    + F.col("pct_with_project_name")
                    + F.col("pct_with_developer_name")
                    + F.col("pct_with_landmark")
                    + F.col("pct_with_description")
                    + F.col("pct_with_amenities")
                    + F.col("pct_with_lux_amenities")
                    + F.col("pct_with_floor_info")
                    + F.col("pct_with_bathrooms")
                    + F.col("pct_with_covered_area")
                    + F.col("pct_with_carpet_area")
                    + F.col("pct_with_owner_name")
                    + F.col("pct_with_company_name")
                    + F.col("pct_usable_for_ppsf_benchmark")
                ) / F.lit(15.0),
                2
            )
        )
        .withColumn(
            "quality_band",
            F.when(F.col("quality_score") >= 85, F.lit("High"))
             .when(F.col("quality_score") >= 65, F.lit("Medium"))
             .otherwise(F.lit("Low"))
        )
    )
