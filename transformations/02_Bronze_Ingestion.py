# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Configurations

CATALOG = spark.conf.get("catalog", "adb_real_estate_mb")
RAW_SCHEMA = "raw"
BRONZE_LISTINGS_RAW = f"{CATALOG}.bronze.mb_listings_raw"

SOURCE_CONFIGS = {
    "ahmedabad": {"volume_name": "local_upload_amd", "city": "Ahmedabad", "source_name": "local_upload_amd", "source_type": "local_upload"},
    "hyderabad": {"volume_name": "aws_s3_hyd", "city": "Hyderabad", "source_name": "aws_s3_hyd", "source_type": "aws_s3"},
    "bangalore": {"volume_name": "azure_blob_bangalore", "city": "Bangalore", "source_name": "azure_blob_bangalore", "source_type": "azure_blob"},
    "chennai": {"volume_name": "github_chennai", "city": "Chennai", "source_name": "github_chennai", "source_type": "github"},
    "mumbai": {"volume_name": "google_drive_mumbai", "city": "Mumbai", "source_name": "google_drive_mumbai", "source_type": "google_drive"},
    "delhi_ncr": {"volume_name": "rest_api_delhi_ncr", "city": "Delhi NCR", "source_name": "rest_api_delhi_ncr", "source_type": "rest_api"},
}

# Helper Functions

def volume_path(volume_name, subfolder="listings"):
    return f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{volume_name}/{subfolder}"


def schema_location(volume_name, city):
    return (
        f"/Volumes/{CATALOG}/{RAW_SCHEMA}/"
        f"{volume_name}/_checkpoints/schema_{city.lower().replace(' ', '_')}"
    )

# Bronze Streaming Table

dp.create_streaming_table(
    name=BRONZE_LISTINGS_RAW,
    comment="""
    Bronze Raw Layer

    One row = One scraped JSON file.

    Contains:
    - Raw JSON
    - File metadata
    - Source metadata
    - City metadata
    - Ingestion timestamp
    """,
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "pipelines.autoOptimize.managed": "true",
    },
    expect_all={
        "raw_json_not_empty":
            "raw_json IS NOT NULL AND LENGTH(TRIM(raw_json)) > 0",

        "city_present":
            "city IS NOT NULL",

        "source_file_present":
            "source_file_name IS NOT NULL",

        "source_path_present":
            "source_file_path IS NOT NULL",

        "ingestion_time_present":
            "ingestion_time IS NOT NULL",
    }
)

# Generic Bronze Reader

def build_flow_df(
    volume_name: str,
    city: str,
    source_name: str,
    source_type: str
):
    """
    Reads JSON files as raw text using Auto Loader.

    One JSON file = One Bronze row.
    """

    raw = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "text")
            .option("wholetext", "true")
            .option(
                "cloudFiles.schemaLocation",
                schema_location(volume_name, city)
            )
            .load(volume_path(volume_name))
    )

    return raw.select(
        F.col("value").alias("raw_json"),

        F.col("_metadata.file_name").alias("source_file_name"),
        F.col("_metadata.file_path").alias("source_file_path"),
        F.col("_metadata.file_size").alias("source_file_size"),
        F.col("_metadata.file_modification_time").alias("source_mod_time"),

        F.lit(city).alias("city"),
        F.lit(source_name).alias("source_name"),
        F.lit(source_type).alias("source_type"),

        F.current_timestamp().alias("ingestion_time")
    )


# ============================================================
# Ahmedabad
# ============================================================

@dp.append_flow(
    target=BRONZE_LISTINGS_RAW,
    name="ingest_ahmedabad"
)
def ingest_ahmedabad():

    return build_flow_df(**SOURCE_CONFIGS["ahmedabad"])


# ============================================================
# Hyderabad
# ============================================================

@dp.append_flow(
    target=BRONZE_LISTINGS_RAW,
    name="ingest_hyderabad"
)
def ingest_hyderabad():

    return build_flow_df(**SOURCE_CONFIGS["hyderabad"])


# ============================================================
# Bangalore
# ============================================================

@dp.append_flow(
    target=BRONZE_LISTINGS_RAW,
    name="ingest_bangalore"
)
def ingest_bangalore():

    return build_flow_df(**SOURCE_CONFIGS["bangalore"])


# ============================================================
# Chennai
# ============================================================

@dp.append_flow(
    target=BRONZE_LISTINGS_RAW,
    name="ingest_chennai"
)
def ingest_chennai():

    return build_flow_df(**SOURCE_CONFIGS["chennai"])


# ============================================================
# Mumbai
# ============================================================

@dp.append_flow(
    target=BRONZE_LISTINGS_RAW,
    name="ingest_mumbai"
)
def ingest_mumbai():

    return build_flow_df(**SOURCE_CONFIGS["mumbai"])


# ============================================================
# Delhi NCR
# ============================================================

@dp.append_flow(
    target=BRONZE_LISTINGS_RAW,
    name="ingest_delhi_ncr"
)
def ingest_delhi_ncr():

    return build_flow_df(**SOURCE_CONFIGS["delhi_ncr"])
