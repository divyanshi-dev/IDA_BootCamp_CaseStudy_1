# Databricks notebook source
storageAccountName = "de10692367dl"
container_name = "divyanshi"
mountPoint = "/mnt/divyanshi/"

if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(container_name, storageAccountName),
      mount_point = mountPoint,
      extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': dbutils.secrets.get(scope="divyanshi-scope", key="divyanshi-adls-sas-key")}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

# Verify the contents of the directory
display(dbutils.fs.ls(mountPoint + "/case_study_1/PROJECT/P00002-ENRICHED/Unharmonised/NonSensitive/3rd_Party/ORCHESTR8/O8_MASTER_DATA/"))

# Correct path for o8_master_data
o8_master_data_path = mountPoint + "/case_study_1/PROJECT/P00002-ENRICHED/Unharmonised/NonSensitive/3rd_Party/ORCHESTR8/O8_MASTER_DATA/"

# Read the entire o8_master_data table, including all partitions
o8_master_data = spark.read.format("delta").load(o8_master_data_path)
o8_master_data.show()



# COMMAND ----------

# Correct path for vendor_master_data
vendor_master_data_path = mountPoint + "/case_study_1/PROJECT/P00002-ENRICHED/Unharmonised/NonSensitive/1st_Party/GSAP_DS_HANA/VENDOR_MASTER/"

# Read the vendor_master_data table
vendor_master_data = spark.read.format("delta").load(vendor_master_data_path)
vendor_master_data.show()

# COMMAND ----------

o8_master_data.display()

# COMMAND ----------

vendor_master_data.display()

# COMMAND ----------

joined_master_df = o8_master_data.join(vendor_master_data, o8_master_data.SupplierCode == vendor_master_data.VendorAccountNumber, "left")


# COMMAND ----------

joined_master_df.display()

# COMMAND ----------

# Select relevant columns from the joined DataFrame
joined_master_df = joined_master_df.select("PartNumber", "MaterialDescription", "SupplierCode", "VendorName", "VendorAddress", "VendorCity", "VendorPostalCode")

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

import json
from pyspark.sql.types import StructType, StructField, StringType

# Display the contents of the directory
display(dbutils.fs.ls("/mnt/divyanshi/case_study_1/"))

# Deserialize the JSON string to a list
file_paths_list = json.loads(filePaths)

print(file_paths_list)

enriched_unharmonised_paths = [(file_path["ObjectName"], file_path["EnrichedUnharmonisedPath"]) for file_path in file_paths_list]

# Create a DataFrame from the extracted paths
schema = StructType([
    StructField("ObjectName", StringType(), True),
    StructField("EnrichedUnharmonisedPath", StringType(), True)
])

enriched_unharmonised_paths_df = spark.createDataFrame(enriched_unharmonised_paths, schema)

# Show the DataFrame
enriched_unharmonised_paths_df.show()

# Iterate over the paths and save data to Delta Lake format
for object_name, path in enriched_unharmonised_paths:
    absolute_path = "/mnt/" + path.strip()
    if object_name == "o8_master_data":
        o8_master_data.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(absolute_path)
    elif object_name == "vendor_master_data":
        vendor_master_data.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(absolute_path)

print("Data transformation and write to Delta Lake format completed successfully.")


# COMMAND ----------

print(file_paths_list)

# COMMAND ----------

o8_master_data.show()

# COMMAND ----------

vendor_master_data.show()

# COMMAND ----------

joined_master_df = o8_master_data.join(vendor_master_data, o8_master_data.SupplierCode == vendor_master_data.VendorAccountNumber, "left")


# COMMAND ----------

# Show the result
joined_master_df.show()

# COMMAND ----------

import json

from pyspark.sql.types import StructType, StructField, StringType


# Get the filePaths parameter from dbutils.widgets
file_paths_json = dbutils.widgets.get("filePaths")

# Deserialize the JSON string to a list
file_paths_list = json.loads(file_paths_json)

# Extract AcquisitionPaths
enriched_harmonised_paths = [(file_path["ObjectName"], file_path["EnrichedHarmonisedPath"]) for file_path in file_paths_list]

# Define schema for the DataFrame
schema = StructType([
    StructField("ObjectName", StringType(), True),
    StructField("EnrichedHarmonisedPath", StringType(), True)
])

# Create DataFrame from the list of acquisition paths
enriched_harmonised_paths_df = spark.createDataFrame(enriched_harmonised_paths, schema)

# Show the DataFrame
enriched_harmonised_paths_df.show()




# COMMAND ----------

import json
import logging
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Example enrichment function
def enrich_data(df):
    # Add a new column with a constant value
    enriched_df = df.withColumn("Enriched", lit("EnrichedValue"))
    # Perform other transformations as needed
    return enriched_df
  
# Save the joined DataFrame to the specified path in Delta format from the EnrichedHarmonisedPath DataFrame
for row in enriched_harmonised_paths_df.collect():
    if row['ObjectName'] == 'o8_master_data':
        enriched_path = f"/mnt/{row['EnrichedHarmonisedPath']}"
        joined_master_df.write.format("delta").mode("overwrite").save(enriched_path)
        logger.info(f"Enriched Delta table created at {enriched_path}")

        # Create an external table for the joined data
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS joined_master_data_enriched_div
            USING DELTA
            LOCATION '{enriched_path}'
        """)
        logger.info(f"External table 'joined_master_data_enriched' created at {enriched_path}")
