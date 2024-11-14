# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

filePaths= dbutils.widgets.get("filePaths")

# COMMAND ----------

print(filePaths)

# COMMAND ----------

# List files in the root of the mounted divyanshi directory
divyanshi_root = "/mnt/divyanshi/"
files = dbutils.fs.ls(divyanshi_root)
for file in files:
    print(file.path)


# COMMAND ----------

display(dbutils.fs.ls("/mnt/divyanshi/case_study_1/"))

# COMMAND ----------

import json

from pyspark.sql.types import StructType, StructField, StringType


# Get the filePaths parameter from dbutils.widgets
file_paths_json = dbutils.widgets.get("filePaths")

# Deserialize the JSON string to a list
file_paths_list = json.loads(file_paths_json)

# Extract AcquisitionPaths
acquisition_paths = [(file_path["ObjectName"], file_path["AcquisitionPath"]) for file_path in file_paths_list]

# Define schema for the DataFrame
schema = StructType([
    StructField("ObjectName", StringType(), True),
    StructField("AcquisitionPath", StringType(), True)
])

# Create DataFrame from the list of acquisition paths
acquisition_paths_df = spark.createDataFrame(acquisition_paths, schema)

# Show the DataFrame
acquisition_paths_df.show()




# COMMAND ----------

import json
import logging
import glob
import os
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the filePaths parameter from dbutils.widgets
file_paths_json = dbutils.widgets.get("filePaths")

# Log the received JSON string for debugging
logger.info("Received file paths JSON: %s", file_paths_json)

# Deserialize the JSON string to a list
file_paths_list = json.loads(file_paths_json)

# Extract AcquisitionPaths
acquisition_paths = [(file_path["ObjectName"], file_path["AcquisitionPath"]) for file_path in file_paths_list]

# Function to list files in a directory
def list_files_in_directory(path):
    try:
        files = dbutils.fs.ls(f"/mnt/{path}")
        if not files:
            logger.warning("No files found in %s", path)
        else:
            logger.info("Files in %s: %s", path, [file.name for file in files])
    except Exception as e:
        logger.error("Error listing files in %s: %s", path, e)

# List files in each AcquisitionPath
for objectname, path in acquisition_paths:
    list_files_in_directory(path)

# Function to read all Parquet files from a given path
def read_all_parquet_files(path):
    try:
        # Use dbutils to get all Parquet files in the directory
        parquet_files = [file.path for file in dbutils.fs.ls(f"/mnt/{path}") if file.path.endswith(".parquet")]
        if not parquet_files:
            logger.warning("No Parquet files found in %s", path)
            return None
        # Read all Parquet files into a single DataFrame
        return spark.read.parquet(*parquet_files)
    except Exception as e:
        logger.error("Error reading Parquet files from %s: %s", path, e)
        return None

# Dictionary to store DataFrames
parquet_dfs = {}

# Read all Parquet files from each AcquisitionPath
for objectname, path in acquisition_paths:
    df = read_all_parquet_files(path)
    if df is not None:
        parquet_dfs[path] = df

# Example: Accessing specific DataFrames
for path, df in parquet_dfs.items():
    logger.info("DataFrame for %s:", path)
    df.show()


# COMMAND ----------

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the filePaths parameter from dbutils.widgets
file_paths_json = dbutils.widgets.get("filePaths")

# Log the received JSON string for debugging
logger.info("Received file paths JSON: %s", file_paths_json)

# Deserialize the JSON string to a list
file_paths_list = json.loads(file_paths_json)

# Extract AcquisitionPaths
acquisition_paths = [(file_path["ObjectName"], file_path["AcquisitionPath"]) for file_path in file_paths_list]

# Function to list files in a directory
def list_files_in_directory(path):
    try:
        files = dbutils.fs.ls(f"/mnt/{path}")
        if not files:
            logger.warning("No files found in %s", path)
        else:
            logger.info("Files in %s: %s", path, [file.name for file in files])
    except Exception as e:
        logger.error("Error listing files in %s: %s", path, e)

# List files in each AcquisitionPath
for objectname, path in acquisition_paths:
    list_files_in_directory(path)

# Function to read specific Parquet files from a given path based on a filter
def read_filtered_parquet_files(path, filter_str):
    try:
        # Use dbutils to get all Parquet files in the directory
        parquet_files = [file.path for file in dbutils.fs.ls(f"/mnt/{path}") if file.path.endswith(".parquet") and filter_str in file.name]
        if not parquet_files:
            logger.warning("No Parquet files found in %s with filter %s", path, filter_str)
            return None
        # Read all filtered Parquet files into a single DataFrame
        return spark.read.parquet(*parquet_files)
    except Exception as e:
        logger.error("Error reading Parquet files from %s with filter %s: %s", path, filter_str, e)
        return None

# Dictionary to store DataFrames
parquet_dfs = {}

# Read specific Parquet files from each AcquisitionPath based on filters
for objectname, path in acquisition_paths:
    if objectname == "o8_master_data":
        df_10_09 = read_filtered_parquet_files(path, "10_09")
        df_26_08 = read_filtered_parquet_files(path, "26_08")
        if df_10_09 is not None:
            parquet_dfs[f"{objectname}_10_09"] = df_10_09
        if df_26_08 is not None:
            parquet_dfs[f"{objectname}_26_08"] = df_26_08
    else:
        df = read_all_parquet_files(path)
        if df is not None:
            parquet_dfs[objectname] = df

# Example: Accessing specific DataFrames
for key, df in parquet_dfs.items():
    logger.info("DataFrame for %s:", key)
    df.show()


# COMMAND ----------

parquet_dfs.keys()

# COMMAND ----------

# Assuming the parquet_dfs dictionary contains 3 DataFrames
# Extracting the DataFrames into separate variables

o8_master_data_10_09 = parquet_dfs[list(parquet_dfs.keys())[0]]
o8_master_data_26_08 = parquet_dfs[list(parquet_dfs.keys())[1]]
vendor_master_data = parquet_dfs[list(parquet_dfs.keys())[2]]

# Display the DataFrames to
#  verify
print("DataFrame 1:")
o8_master_data_10_09.show()

print("DataFrame 2:")
o8_master_data_26_08.show()

print("DataFrame 3:")
vendor_master_data.show()


# COMMAND ----------

import json

from pyspark.sql.types import StructType, StructField, StringType


# Get the filePaths parameter from dbutils.widgets
file_paths_json = dbutils.widgets.get("filePaths")

# Deserialize the JSON string to a list
file_paths_list = json.loads(file_paths_json)

# Extract AcquisitionPaths
enriched_unharmonised_paths = [(file_path["ObjectName"], file_path["EnrichedUnharmonisedPath"]) for file_path in file_paths_list]

# Define schema for the DataFrame
schema = StructType([
    StructField("ObjectName", StringType(), True),
    StructField("EnrichedUnharmonisedPath", StringType(), True)
])

# Create DataFrame from the list of acquisition paths
enriched_unharmonised_paths_df = spark.createDataFrame(enriched_unharmonised_paths, schema)

# Show the DataFrame
enriched_unharmonised_paths_df.show()




# COMMAND ----------

# Merge the DataFrames
o8_master_data = o8_master_data_26_08.union(o8_master_data_10_09)

# Display the merged DataFrame
o8_master_data.show()

# COMMAND ----------

o8_master_data.display()

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("MergeDataFrames").getOrCreate()



# Merge the DataFrames
merged_df = o8_master_data_26_08.union(o8_master_data_10_09)

# Remove duplicates from the merged DataFrame
deduplicated_df = merged_df.dropDuplicates()

# Display the deduplicated DataFrame
deduplicated_df.show()


# COMMAND ----------

o8_master_data = deduplicated_df

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType
import re

# Initialize Spark session
spark = SparkSession.builder.appName("NonEnglishCharacterCheck").getOrCreate()

# Define a function to check for non-English characters
def has_non_english_characters(text):
    if text is None:
        return False
    return bool(re.search(r'[^a-zA-Z0-9\s]', text))

# Register the function as a UDF
has_non_english_characters_udf = udf(has_non_english_characters, BooleanType())

# Apply the function to the relevant columns in o8_master_data
o8_master_data = o8_master_data.withColumn("has_non_english", 
    has_non_english_characters_udf(col("MATERIAL_DESC")) |
    has_non_english_characters_udf(col("O8_SUPPLIER_CODE")) |
    has_non_english_characters_udf(col("O8_PROCUREMENT_TYPE_DESC")) |
    has_non_english_characters_udf(col("O8_REPLENISHMENT_LEAD_TIME")) |
    has_non_english_characters_udf(col("O8_PART_TYPE_CODE")) |
    has_non_english_characters_udf(col("O8_ACTIVE_PART")) |
    has_non_english_characters_udf(col("MATERIAL_TYPE_CODE")) |
    has_non_english_characters_udf(col("MRP_CONTROLLER_CODE")) |
    has_non_english_characters_udf(col("O8_PART_STATUS")) |
    has_non_english_characters_udf(col("PRODUCT_HIERARCHY_SKU_CODE")) |
    has_non_english_characters_udf(col("MATERIAL_GROUP_CODE")) |
    has_non_english_characters_udf(col("PLANNING_STRATEGY_GROUP_CODE")) |
    has_non_english_characters_udf(col("O8_SOURCE_LOCATION"))
)

# Calculate the percentage of records with non-English characters
total_records = o8_master_data.count()
non_english_records = o8_master_data.filter(col("has_non_english")).count()
percentage_non_english = (non_english_records / total_records) * 100

print(f"Percentage of records with non-English characters in o8_master_data: {percentage_non_english:.2f}%")

# Apply the function to the relevant columns in vendor_master_data

vendor_master_data = vendor_master_data.withColumn("has_non_english", 
    has_non_english_characters_udf(col("VENDOR_NAME_1")) |
    has_non_english_characters_udf(col("VENDOR_ADDRESS")) |
    has_non_english_characters_udf(col("VENDOR_CITY_NAME")) |
    has_non_english_characters_udf(col("VENDOR_POSTAL_CODE")) |
    has_non_english_characters_udf(col("VENDOR_RECORD_CREATE_BY"))
)

# Calculate the percentage of records with non-English characters
total_records_vendor = vendor_master_data.count()
non_english_records_vendor = vendor_master_data.filter(col("has_non_english")).count()
percentage_non_english_vendor = (non_english_records_vendor / total_records_vendor) * 100

print(f"Percentage of records with non-English characters in vendor_master_data: {percentage_non_english_vendor:.2f}%")


# COMMAND ----------

# Find columns where '?' is written because of encoding issues using regex
columns_with_encoding_issues = []
for column in merged_df.columns:
    if o8_master_data.filter(col(column).rlike(r'\?+')).count() > 0:
        columns_with_encoding_issues.append(column)

# Display the columns with encoding issues
print("Columns with encoding issues (containing '?'):")
for column in columns_with_encoding_issues:
    print(column)

# COMMAND ----------

# Calculate the percentage of '?' in the rows
total_records = o8_master_data.count()
columns_with_encoding_issues = []
for column in o8_master_data.columns:
    count_encoding_issues = o8_master_data.filter(col(column).rlike(r'\?+')).count()
    if count_encoding_issues > 0:
        columns_with_encoding_issues.append((column, count_encoding_issues))

# Calculate and display the percentage of '?' in the rows for each column with encoding issues
for column, count_encoding_issues in columns_with_encoding_issues:
    percentage_encoding_issues = (count_encoding_issues / total_records) * 100
    print(f"Column '{column}' has {percentage_encoding_issues:.2f}% records with encoding issues ('?').")


# COMMAND ----------

from pyspark.sql.functions import col, when

# Replace '?' in the rows with null
for column in o8_master_data.columns:
    o8_master_data = o8_master_data.withColumn(column, when(col(column).rlike(r'\?+'), None).otherwise(col(column)))

# Display the DataFrame to verify
o8_master_data.show()

# COMMAND ----------

# Calculate the percentage of '?' in the rows
total_records = o8_master_data.count()
columns_with_encoding_issues = []
for column in o8_master_data.columns:
    count_encoding_issues = o8_master_data.filter(col(column).rlike(r'\?+')).count()
    if count_encoding_issues > 0:
        columns_with_encoding_issues.append((column, count_encoding_issues))

# Calculate and display the percentage of '?' in the rows for each column with encoding issues
for column, count_encoding_issues in columns_with_encoding_issues:
    percentage_encoding_issues = (count_encoding_issues / total_records) * 100
    print(f"Column '{column}' has {percentage_encoding_issues:.2f}% records with encoding issues ('?').")

print('No columns with ?')

# COMMAND ----------

from pyspark.sql.functions import col


# Step 1: Assign Business-Relevant Names to Source Columns
o8_master_data = o8_master_data.withColumnRenamed("O8_PART_NO", "PartNumber") \
    .withColumnRenamed("MATERIAL_DESC", "MaterialDescription") \
    .withColumnRenamed("O8_SUPPLIER_CODE", "SupplierCode") \
    .withColumnRenamed("O8_PROCUREMENT_TYPE_DESC", "ProcurementTypeDescription") \
    .withColumnRenamed("O8_REPLENISHMENT_LEAD_TIME", "ReplenishmentLeadTime") \
    .withColumnRenamed("MAX_LOT_SIZE", "MaxLotSize") \
    .withColumnRenamed("O8_MIN_LOT_SIZE", "MinLotSize") \
    .withColumnRenamed("O8_ORDER_QTY_IN_RUOM_ROUNDING_VALUE", "OrderQuantityRoundingValue") \
    .withColumnRenamed("O8_PHASE_IN_DATE", "PhaseInDate") \
    .withColumnRenamed("O8_PHASE_OUT_DATE", "PhaseOutDate") \
    .withColumnRenamed("O8_PART_TYPE_CODE", "PartTypeCode") \
    .withColumnRenamed("STANDARD_PRICE", "StandardPrice") \
    .withColumnRenamed("O8_ACTIVE_PART", "ActivePart") \
    .withColumnRenamed("MATERIAL_TYPE_CODE", "MaterialTypeCode") \
    .withColumnRenamed("MRP_CONTROLLER_CODE", "MRPControllerCode") \
    .withColumnRenamed("O8_NO_OF_DAYS_STRATEGIC_BUFFER", "StrategicBufferDays") \
    .withColumnRenamed("PALLET_QTY", "PalletQuantity") \
    .withColumnRenamed("O8_PART_STATUS", "PartStatus") \
    .withColumnRenamed("PRODUCT_HIERARCHY_SKU_CODE", "ProductHierarchySKUCode") \
    .withColumnRenamed("MATERIAL_GROUP_CODE", "MaterialGroupCode") \
    .withColumnRenamed("PLANNING_STRATEGY_GROUP_CODE", "PlanningStrategyGroupCode") \
    .withColumnRenamed("O8_SOURCE_LOCATION", "SourceLocation") \
    .withColumnRenamed("PLANT_CODE", "PlantCode")



# COMMAND ----------

# Step 2: Retain Only Business-Required Columns
o8_master_data = o8_master_data.select("PartNumber", "MaterialDescription", "SupplierCode", "ProcurementTypeDescription", 
                                             "ReplenishmentLeadTime", "MaxLotSize", "MinLotSize", "OrderQuantityRoundingValue", 
                                             "PhaseInDate", "PhaseOutDate", "PartTypeCode", "StandardPrice", "ActivePart", 
                                             "MaterialTypeCode", "MRPControllerCode", "StrategicBufferDays", "PalletQuantity", 
                                             "PartStatus", "ProductHierarchySKUCode", "MaterialGroupCode", "PlanningStrategyGroupCode", 
                                             "SourceLocation", "PlantCode")


# COMMAND ----------

vendor_master_data = vendor_master_data.withColumnRenamed("VENDOR_OR_CREDITOR_ACCOUNT_NO", "VendorAccountNumber") \
    .withColumnRenamed("VENDOR_NAME_1", "VendorName") \
    .withColumnRenamed("VENDOR_ADDRESS", "VendorAddress") \
    .withColumnRenamed("VENDOR_CITY_NAME", "VendorCity") \
    .withColumnRenamed("VENDOR_POSTAL_CODE", "VendorPostalCode") \
    .withColumnRenamed("VENDOR_RECORD_CREATE_BY", "RecordCreatedBy")

# COMMAND ----------

vendor_master_data = vendor_master_data.select("VendorAccountNumber", "VendorName", "VendorAddress", "VendorCity", 
                                                     "VendorPostalCode", "RecordCreatedBy")



# COMMAND ----------

# Extract AcquisitionPaths
enriched_unharmonised_paths = [(file_path["ObjectName"], file_path["EnrichedUnharmonisedPath"]) for file_path in file_paths_list]

# Assuming enriched_unharmonised_paths_df is already created and contains the paths
enriched_unharmonised_paths_df.show()

# Define the base path for your storage
base_path = "/mnt/"

# Enable schema auto merge
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Iterate over the paths and save data to Delta Lake format
for object_name, path in enriched_unharmonised_paths:
    absolute_path = base_path + path.strip()
    if object_name == "o8_master_data":
        o8_master_data.write.format("delta").mode("overwrite").save(absolute_path)
    elif object_name == "vendor_master_data":
        vendor_master_data.write.format("delta").mode("overwrite").save(absolute_path)

print("Data transformation and write to Delta Lake format completed successfully.")


# COMMAND ----------

# import json

# from pyspark.sql.types import StructType, StructField, StringType


# # Get the filePaths parameter from dbutils.widgets
# file_paths_json = dbutils.widgets.get("filePaths")

# # Deserialize the JSON string to a list
# file_paths_list = json.loads(file_paths_json)

# # Extract AcquisitionPaths
# enriched_unharmonised_paths = [(file_path["ObjectName"], file_path["EnrichedUnharmonisedPath"]) for file_path in file_paths_list]

# # Define schema for the DataFrame
# schema = StructType([
#     StructField("ObjectName", StringType(), True),
#     StructField("EnrichedUnharmonisedPath", StringType(), True)
# ])

# # Create DataFrame from the list of acquisition paths
# enriched_unharmonised_paths_df = spark.createDataFrame(enriched_unharmonised_paths, schema)

# # Show the DataFrame
# enriched_unharmonised_paths_df.show()




# COMMAND ----------

# import json
# import logging
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import lit
# from pyspark.sql.types import StructType, StructField, StringType
# from pyspark.sql.utils import AnalysisException



# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # Example enrichment function
# def enrich_data(df):
#     # Add a new column with a constant value
#     enriched_df = df.withColumn("Enriched", lit("EnrichedValue"))
#     # Perform other transformations as needed
#     return enriched_df

# # Enable schema auto merge
# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# enriched_unharmonised_paths_df.show()




# # Enrich the data
# enriched_o8_master_df = enrich_data(o8_master_data)
# enriched_vendor_df = enrich_data(vendor_master_data)

# # Iterate over the paths and save data to Delta Lake format
# for row in enriched_unharmonised_paths_df.collect():
#     object_name = row['ObjectName']
#     path = row['EnrichedUnharmonisedPath']
    
#     if object_name == "o8_master_data":
#         # Save the DataFrame in Delta format partitioned by PlantCode
#         enriched_o8_master_df.write.option("header", True).format("delta").mode("overwrite").partitionBy("PlantCode").save("/mnt/"+path.strip())

#     elif object_name == "vendor_master_data":
#         # Save the DataFrame in Delta format without partitioning
#         enriched_vendor_df.write.format("delta").mode("overwrite").save("/mnt/"+path.strip())

# print("Data transformation and write to Delta Lake format completed successfully.")


# COMMAND ----------


