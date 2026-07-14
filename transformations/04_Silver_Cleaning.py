# Databricks notebook source
# Reads: bronze_flattened
# Writes: silver_cleaned
#
# Implements:
# 1. Deduplication
# 2. Null standardization
# 3. Trim strings
# 4. Remove hidden characters
# 5. Standardize categorical text
# 6. Explicit datatype conversion
# 7. Parse dates
# 8. Clean arrays
# 9. URL cleaning
# 10. Collapse extra spaces
# 11. Address normalization
# 12. Description cleaning
# 13. Remove duplicate image URLs
# 14. Standardize values

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

CATALOG = spark.conf.get("catalog", "adb_real_estate_mb")
BRONZE_FLATTENED = f"{CATALOG}.bronze.bronze_flattened"
SILVER_CLEANED = f"{CATALOG}.silver.silver_cleaned"

# -----------------------------
# Column groups
# -----------------------------

STRING_COLUMNS = [
"source_file_name","source_file_path","source_city","source_name","source_type",
"listing_id","enc_id","url","seo_url","property_title","city_name",
"locality_name","locality_seo_name","address_google","address_full","landmark",
"covered_area_raw","carpet_area_raw","carpet_area_unit","property_type",
"bedrooms","bathrooms","balconies","additional_room","floor_no","total_floors",
"facing","flooring_type","furnished_status","transaction_type",
"transaction_category","possession_status","available_from",
"age_of_construction","ownership_type","water_status","power_status",
"posted_date_raw","posted_label","last_access_date_raw",
"modified_date_raw","last_contacted_date_raw","user_type",
"owner_id","owner_name","company_name","contact_name","rera_validity",
"dev_id","dev_name","project_name","project_society_link",
"psm_id","psm_address","image","amenities","lux_amenities",
"auto_desc","seo_desc","detail_desc"
]

CATEGORY_COLUMNS = [
"property_type","furnished_status","transaction_type",
"transaction_category","ownership_type","user_type",
"facing","flooring_type"
]

URL_COLUMNS = [
"url","seo_url","image"
]

ADDRESS_COLUMNS = [
"address_google","address_full","landmark"
]

DESCRIPTION_COLUMNS = [
"auto_desc","seo_desc","detail_desc","property_title"
]

INTEGER_COLUMNS = [
"listing_id","bedrooms","bathrooms","balconies",
"floor_no","total_floors","owner_id","dev_id","psm_id"
]

TIMESTAMP_COLUMNS = [
"posted_date_raw",
"last_access_date_raw",
"last_contacted_date_raw"
]

# -----------------------------
# Helper functions
# -----------------------------

def normalize_null(col):
    return (
        F.when(
            F.trim(F.lower(col)).isin("", "null", "none", "na", "n/a"),
            F.lit(None)
        ).otherwise(col)
    )

def clean_string(col):
    c = normalize_null(col)
    c = F.regexp_replace(c, r"[\r\n\t]", " ")
    c = F.regexp_replace(c, r"\s+", " ")
    c = F.trim(c)
    return c


def clean_json_string_array(col_name):
    raw = F.col(col_name)
    parsed = F.from_json(raw, ArrayType(StringType()))
    fallback = F.array(clean_string(raw))
    values = F.coalesce(parsed, fallback)
    return F.when(raw.isNull(), F.lit(None).cast(ArrayType(StringType()))).otherwise(
        F.array_distinct(
            F.filter(
                F.transform(values, lambda value: clean_string(value)),
                lambda value: value.isNotNull(),
            )
        )
    )


def parse_modified_timestamp():
    raw = F.lower(F.trim(F.col("modified_date_raw")))
    source_date = F.to_date("source_file_modification_time")
    return (
        F.when(raw == "today", F.to_timestamp(source_date))
        .when(raw == "yesterday", F.to_timestamp(F.date_sub(source_date, 1)))
        .otherwise(
            F.coalesce(
                F.to_timestamp("modified_date_raw", "MMM dd, ''yy"),
                F.to_timestamp("modified_date_raw", "MMM ''yy"),
                F.to_timestamp("modified_date_raw"),
            )
        )
    )

# -----------------------------
# Silver Table
# -----------------------------

dp.create_streaming_table(
    name=SILVER_CLEANED,
    comment="Cleaned Silver table",
    table_properties={"quality":"silver"}
)

@dp.temporary_view(name="silver_cleaned_source")
def silver_cleaning_source():

    df = spark.readStream.table(BRONZE_FLATTENED)

    # ==========================================================
    # Step 2-5 : Generic String Cleaning
    # ==========================================================

    for c in STRING_COLUMNS:
        if c in df.columns:
            df = df.withColumn(c, clean_string(F.col(c)))

    df = df.withColumn("modified_ts", parse_modified_timestamp())

    # ==========================================================
    # Step 6 : Datatype Conversion
    # ==========================================================

    for c in INTEGER_COLUMNS:
        if c in df.columns:
            df = df.withColumn(c,F.col(c).cast("long"))

    # ==========================================================
    # Step 7 : Timestamp Parsing
    # ==========================================================

    for c in TIMESTAMP_COLUMNS:
        if c in df.columns:
            df = (
                df.withColumn(
                    c,
                    F.coalesce(
                        F.to_timestamp(c),
                        F.to_timestamp(c,"yyyy-MM-dd HH:mm:ss"),
                        F.to_timestamp(c,"yyyy-MM-dd'T'HH:mm:ss.SSSX")
                    )
                )
            )

    # Available From
    if "available_from" in df.columns:
        df = (
            df
            .withColumn(
                "available_from",
                F.when(
                    F.col("available_from").isNull(),
                    None
                ).otherwise(
                    F.to_date(
                        F.concat(F.lit("01 "),F.col("available_from")),
                        "dd MMM ''yy"
                    )
                )
            )
        )

    # Parse the JSON-encoded amenity arrays emitted by Magicbricks.
    for c in ("amenities", "lux_amenities"):
        if c in df.columns:
            df = df.withColumn(c, clean_json_string_array(c))

    # ==========================================================
    # Step 9 : URL Cleaning
    # ==========================================================

    for c in URL_COLUMNS:
        if c in df.columns:
            df = (
                df
                .withColumn(c,F.trim(F.col(c)))
                .withColumn(c,F.regexp_replace(c,r"\s+",""))
            )

    # ==========================================================
    # Step 10 : Address Cleaning
    # ==========================================================

    for c in ADDRESS_COLUMNS:
        if c in df.columns:
            df = (
                df.withColumn(
                    c,
                    F.regexp_replace(c,r"\s+,",",")
                )
            )

    # ==========================================================
    # Step 11 : Description Cleaning
    # ==========================================================

    for c in DESCRIPTION_COLUMNS:
        if c in df.columns:
            df = (
                df
                .withColumn(
                    c,
                    F.regexp_replace(c,r"<br\s*/?>"," ")
                )
                .withColumn(
                    c,
                    F.regexp_replace(c,r"\s+"," ")
                )
            )

    # ==========================================================
    # Step 12 : Remove duplicate image URLs
    # ==========================================================

    if "all_image_paths" in df.columns:
        df = (
            df.withColumn(
                "all_image_paths",
                F.array_distinct("all_image_paths")
            )
        )

    # ==========================================================
    # Step 13 : Category Standardization
    # ==========================================================

    for c in CATEGORY_COLUMNS:
        if c in df.columns:
            df = (
                df.withColumn(
                    c,
                    F.initcap(F.lower(F.col(c)))
                )
            )

    # ==========================================================
    # Step 14 : Standardize price
    # ==========================================================

    df = df.withColumn("price",F.col("price").cast("long"))

    return df


# ==========================================================
# Stream cleaned records into Silver
# No deduplication
# No CDC
# Every cleaned Bronze record is preserved
# ==========================================================

@dp.append_flow(
    target=SILVER_CLEANED,
    name="silver_cleaning"
)
def silver_cleaning():
    return spark.readStream.table("silver_cleaned_source")
