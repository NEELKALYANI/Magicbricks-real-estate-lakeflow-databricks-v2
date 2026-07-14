# Databricks notebook source
# One-time / idempotent catalog setup.
# Creates the raw/bronze/silver/gold schemas and the raw-layer volumes
# used by the ingestion notebooks and the Lakeflow pipeline.
#
# Safe to run repeatedly: every statement is IF NOT EXISTS.
# Catalog itself is NOT created here — Unity Catalog catalog creation
# is an account/metastore-level action and is intentionally left as a
# manual one-time step (see README).

dbutils.widgets.text("catalog", "adb_real_estate_mb")
CATALOG = dbutils.widgets.get("catalog")

SCHEMAS = ["raw", "bronze", "silver", "gold"]

# volume_name -> matches the volume_name values used in
# transformations/02_Bronze_Ingestion.py SOURCE_CONFIGS, plus the
# rest_api Files-API upload target. Keep these two lists in sync.
VOLUMES = [
    "local_upload_amd",
    "github_chennai",
    "google_drive_mumbai",
    "azure_blob_bangalore",
    "aws_s3_hyd",
    "rest_api_delhi_ncr",
]

for schema in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"[OK] schema {CATALOG}.{schema}")

for volume in VOLUMES:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.{volume}")
    print(f"[OK] volume {CATALOG}.raw.{volume}")

print("\nSetup complete.")
