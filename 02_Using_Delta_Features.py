# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG maxkoehler_demos;
# MAGIC USE SCHEMA reading_changes;

# COMMAND ----------

# Get the latest commit version from the Delta table
latest_version = spark.sql("DESCRIBE HISTORY material_number_demand_source LIMIT 1").select("version").collect()[0][0]

print(latest_version)

# COMMAND ----------

# Calculate the second to last commit version
second_to_last_version = latest_version - 1

# Read the changes from the Delta table since the second to last commit version
changes_df = (
    spark.readStream.format("delta")
    .option("readChangeData", "true")
    #.option("startingVersion", second_to_last_version) #if you want to specify a starting version or time point
    .table("material_number_demand_source")
)

display(changes_df)

# COMMAND ----------

# You can do transformations in this stream
from pyspark.sql.functions import lit
changes_df = changes_df.withColumn("customer", lit("my_customer")).select("material_number", "plant", "demand", "customer")
display(changes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC This code will merge the changes from the streaming DataFrame changes_df into the Delta table material_number_demand_target in a batch manner. The foreachBatch method allows you to apply custom logic (in this case, the merge operation) to each micro-batch of the streaming DataFrame.

# COMMAND ----------

from delta.tables import DeltaTable

# Define the target Delta table
target_table = DeltaTable.forName(spark, "material_number_demand_target_1")

# Define the merge function
def merge_changes(batch_df, batch_id):
    target_table.alias("target").merge(
        batch_df.alias("source"),
        "target.material_number = source.material_number"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()

# Apply the merge function to each micro-batch
changes_df.writeStream.foreachBatch(merge_changes).start()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from material_number_demand_target_1;

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()

# COMMAND ----------



# COMMAND ----------


