# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr

# Define the source table
@dlt.table
def source_table():
    return (
        spark.readStream
        .format("delta")
        .table("maxkoehler_demos.reading_changes.material_number_demand_source")
    )

# Define the target table
dlt.create_streaming_table(
    name="target_table_dlt_2",
    comment="This table holds the updated data from the source table.",
    table_properties={"quality": "silver"}
)

# Apply changes from the source table to the target table
dlt.apply_changes(
    target="target_table_dlt_2",
    source="source_table",
    keys=["material_number"],
    sequence_by=col("creation_time"),
    #apply_as_deletes=expr("operation = 'DELETE'"),
    #except_column_list=["operation", "creation_time"],
    stored_as_scd_type=2  # Example for SCD type 2
)
