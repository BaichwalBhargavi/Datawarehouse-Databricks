# Databricks notebook source
# MAGIC %md
# MAGIC Create a table in bronze layer called as bronze_source using last_load_date variable

# COMMAND ----------

if spark.catalog.tableExists("datamodeling.bronze.bronze_source"):
    last_load_date = spark.sql("SELECT MAX(order_date) FROM datamodeling.bronze.bronze_source").collect()[0][0]
    print(last_load_date)
    #returns the max date in the before incremntal load
else:
    last_load_date = "1900-01-01"

# COMMAND ----------

# MAGIC %md
# MAGIC Selects data greater than the last_load_date

# COMMAND ----------

spark.sql(f"""Select * from datamodeling.default.source_data where order_date > '{last_load_date}'""").createOrReplaceTempView("bronze_source")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from bronze_source

# COMMAND ----------

# MAGIC %md
# MAGIC Craeted new table in bronze layer called as bronze_source 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.bronze.bronze_source
# MAGIC AS
# MAGIC SELECT * from bronze_source