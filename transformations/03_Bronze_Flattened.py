# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, ArrayType, IntegerType
)

CATALOG = spark.conf.get("catalog", "adb_real_estate_mb")
BRONZE_LISTINGS_RAW = f"{CATALOG}.bronze.mb_listings_raw"
BRONZE_FLATTENED = f"{CATALOG}.bronze.bronze_flattened"

listing_schema = StructType([
    # --- Identifiers ---
    StructField("id", StringType()),
    StructField("encId", StringType()),
    StructField("url", StringType()),
    StructField("seoURL", StringType()),
    StructField("propertyTitle", StringType()),

    # --- Location ---
    StructField("ctName", StringType()),
    StructField("loc", LongType()),
    StructField("lmtDName", StringType()),
    StructField("locSeoName", StringType()),
    StructField("defaultAdddressGoogle", StringType()),
    StructField("catAdd1", StringType()),
    StructField("landmark", StringType()),
    StructField("pmtLat", DoubleType()),
    StructField("pmtLong", DoubleType()),
    StructField("landmarkDetails", ArrayType(StringType())),

    # --- Price / Area ---
    StructField("price", LongType()),
    StructField("priceD", StringType()),
    StructField("sqFtPrice", DoubleType()),
    StructField("minPrice", LongType()),
    StructField("maxPrice", LongType()),
    StructField("caSqFt", DoubleType()),
    StructField("coveredArea", StringType()),
    StructField("carpetArea", StringType()),
    StructField("carpAreaUnit", StringType()),

    # --- Property specs ---
    StructField("propTypeD", StringType()),
    StructField("bedroomD", StringType()),
    StructField("bathD", StringType()),
    StructField("balconiesD", StringType()),
    StructField("adroomD", StringType()),
    StructField("floorNo", StringType()),
    StructField("floors", StringType()),
    StructField("facingD", StringType()),
    StructField("flooringTyD", StringType()),
    StructField("furnishedD", StringType()),

    # --- Transaction ---
    StructField("transactionTypeD", StringType()),
    StructField("transType", StringType()),
    StructField("possStatusD", StringType()),
    StructField("availableFrom", StringType()),
    StructField("acD", StringType()),
    StructField("OwnershipTypeD", StringType()),
    StructField("waterStatus", StringType()),
    StructField("powerStatusD", StringType()),

    # --- Dates ---
    StructField("postDateT", StringType()),
    StructField("postedLabelD", StringType()),
    StructField("lastAccessDate", StringType()),
    StructField("modifiedDate", StringType()),
    StructField("lCtDate", StringType()),

    # --- Owner / Agent ---
    StructField("userType", StringType()),
    StructField("oid", StringType()),
    StructField("oname", StringType()),
    StructField("companyname", StringType()),
    StructField("contName", StringType()),
    StructField("reraValidity", StringType()),

    # --- Developer / Project ---
    StructField("devId", StringType()),
    StructField("devName", StringType()),
    StructField("prjname", StringType()),
    StructField("projectSocietyLink", StringType()),
    StructField("psmid", StringType()),
    StructField("psmAdd", StringType()),

    # --- Images ---
    StructField("image", StringType()),
    StructField("imgCt", IntegerType()),
    StructField("allImgPath", ArrayType(StringType())),

    # --- Amenities / Descriptions ---
    StructField("amenities", StringType()),
    StructField("luxAmenitiesD", StringType()),
    StructField("psmAmenDesc", ArrayType(StringType())),
    StructField("auto_desc", StringType()),
    StructField("seoDesc", StringType()),
    StructField("dtldesc", StringType()),
])

raw_json_schema = StructType([
    StructField("resultList", ArrayType(listing_schema))
])


dp.create_streaming_table(
    name=BRONZE_FLATTENED,
    comment="One row per listing, exploded from bronze.raw_json.resultList. No validation/DQ checks here — that lives in silver.",
    table_properties={
        "quality": "bronze",
    }
)

@dp.append_flow(target=BRONZE_FLATTENED, name="flatten_bronze")
def flatten_bronze():
    df = spark.readStream.table(BRONZE_LISTINGS_RAW)

    parsed = df.withColumn(
        "parsed",
        F.from_json(
            F.col("raw_json"),
            raw_json_schema,
            options={"rescuedDataColumn": "_rescued_data"}
        )
    )

    exploded = parsed.withColumn(
        "listing",
        F.explode_outer("parsed.resultList")
    )

    flattened = exploded.select(
        F.col("source_file_name"),
        F.col("source_file_path"),
        F.col("source_file_size"),
        F.col("source_mod_time").alias("source_file_modification_time"),
        F.col("city").alias("source_city"),
        F.col("source_name"),
        F.col("source_type"),
        F.col("ingestion_time"),
        F.col("listing.id").alias("listing_id"),
        F.col("listing.encId").alias("enc_id"),
        F.col("listing.url"),
        F.col("listing.seoURL").alias("seo_url"),
        F.col("listing.propertyTitle").alias("property_title"),
        F.col("listing.ctName").alias("city_name"),
        F.col("listing.loc").alias("locality_id"),
        F.col("listing.lmtDName").alias("locality_name"),
        F.col("listing.locSeoName").alias("locality_seo_name"),
        F.col("listing.defaultAdddressGoogle").alias("address_google"),
        F.col("listing.catAdd1").alias("address_full"),
        F.col("listing.landmark"),
        F.col("listing.pmtLat").alias("latitude"),
        F.col("listing.pmtLong").alias("longitude"),
        F.col("listing.landmarkDetails").alias("landmark_details"),
        F.col("listing.price"),
        F.col("listing.priceD").alias("price_display"),
        F.col("listing.sqFtPrice").alias("sqft_price"),
        F.col("listing.minPrice").alias("min_price"),
        F.col("listing.maxPrice").alias("max_price"),
        F.col("listing.caSqFt").alias("covered_area_sqft"),
        F.col("listing.coveredArea").alias("covered_area_raw"),
        F.col("listing.carpetArea").alias("carpet_area_raw"),
        F.col("listing.carpAreaUnit").alias("carpet_area_unit"),
        F.col("listing.propTypeD").alias("property_type"),
        F.col("listing.bedroomD").alias("bedrooms"),
        F.col("listing.bathD").alias("bathrooms"),
        F.col("listing.balconiesD").alias("balconies"),
        F.col("listing.adroomD").alias("additional_room"),
        F.col("listing.floorNo").alias("floor_no"),
        F.col("listing.floors").alias("total_floors"),
        F.col("listing.facingD").alias("facing"),
        F.col("listing.flooringTyD").alias("flooring_type"),
        F.col("listing.furnishedD").alias("furnished_status"),
        F.col("listing.transactionTypeD").alias("transaction_type"),
        F.col("listing.transType").alias("transaction_category"),
        F.col("listing.possStatusD").alias("possession_status"),
        F.col("listing.availableFrom").alias("available_from"),
        F.col("listing.acD").alias("age_of_construction"),
        F.col("listing.OwnershipTypeD").alias("ownership_type"),
        F.col("listing.waterStatus").alias("water_status"),
        F.col("listing.powerStatusD").alias("power_status"),
        F.col("listing.postDateT").alias("posted_date_raw"),
        F.col("listing.postedLabelD").alias("posted_label"),
        F.col("listing.lastAccessDate").alias("last_access_date_raw"),
        F.col("listing.modifiedDate").alias("modified_date_raw"),
        F.col("listing.lCtDate").alias("last_contacted_date_raw"),
        F.col("listing.userType").alias("user_type"),
        F.col("listing.oid").alias("owner_id"),
        F.col("listing.oname").alias("owner_name"),
        F.col("listing.companyname").alias("company_name"),
        F.col("listing.contName").alias("contact_name"),
        F.col("listing.reraValidity").alias("rera_validity"),
        F.col("listing.devId").alias("dev_id"),
        F.col("listing.devName").alias("dev_name"),
        F.col("listing.prjname").alias("project_name"),
        F.col("listing.projectSocietyLink").alias("project_society_link"),
        F.col("listing.psmid").alias("psm_id"),
        F.col("listing.psmAdd").alias("psm_address"),
        F.col("listing.image"),
        F.col("listing.imgCt").alias("image_count"),
        F.col("listing.allImgPath").alias("all_image_paths"),
        F.col("listing.amenities"),
        F.col("listing.luxAmenitiesD").alias("lux_amenities"),
        F.col("listing.psmAmenDesc").alias("psm_amenity_desc"),
        F.col("listing.auto_desc"),
        F.col("listing.seoDesc").alias("seo_desc"),
        F.col("listing.dtldesc").alias("detail_desc"),
    )

    return flattened
