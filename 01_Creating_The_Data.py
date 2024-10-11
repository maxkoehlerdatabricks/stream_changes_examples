# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS maxkoehler_demos;
# MAGIC USE CATALOG maxkoehler_demos;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS reading_changes CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS reading_changes;
# MAGIC USE SCHEMA reading_changes;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE material_number_demand_source (
# MAGIC     material_number STRING,
# MAGIC     plant STRING,
# MAGIC     demand DOUBLE,
# MAGIC     creation_time INTEGER
# MAGIC )
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE material_number_demand_target_1 (
# MAGIC     material_number STRING,
# MAGIC     plant STRING,
# MAGIC     demand DOUBLE,
# MAGIC     customer STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO material_number_demand_source (material_number, plant, demand, creation_time)
# MAGIC VALUES 
# MAGIC     ('M123451', 'Plant1', 100.0, 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from material_number_demand_source

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO material_number_demand_source (material_number, plant, demand, creation_time)
# MAGIC VALUES 
# MAGIC     ('M678904', 'Plant2', 204.0, 2);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from material_number_demand_source

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO material_number_demand_target_1 (material_number, plant, demand, customer)
# MAGIC VALUES 
# MAGIC     ('M123451', 'Plant1', 99.0, "my_customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from material_number_demand_target_1;
