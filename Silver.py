# Databricks notebook source
# MAGIC %md
# MAGIC Check if the correct data is loaded in bronze table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from datamodeling.bronze.bronze_source

# COMMAND ----------

# MAGIC %md
# MAGIC Do some processing in Silver layer ( this processing is done only on the newest records)

# COMMAND ----------

spark.sql("""
          SELECT * ,
            CASE WHEN payment_type = 'Credit Card' THEN 'CARD' ELSE 'UPI' END AS PAYMENT_METHOD,
            date(current_timestamp()) AS PROCESSED_DATE
            FROM datamodeling.bronze.bronze_source
          """).createOrReplaceTempView('silver_source')



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from silver_source

# COMMAND ----------

# MAGIC %md
# MAGIC Create a silver table if it doesnt exist

# COMMAND ----------

if spark.catalog.tableExists("datamodeling.silver.silver_table"):
    pass
else:
    spark.sql("""
          CREATE TABLE IF NOT EXISTS datamodeling.silver.silver_table
          AS
          Select * from silver_source
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC **Merge Using Pyspark**

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE into datamodeling.silver.silver_table
# MAGIC USING silver_source
# MAGIC ON datamodeling.silver.silver_table.order_id = silver_source.order_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC Finally we can see all the records in the Silver table (using Incremental load)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM datamodeling.silver.silver_table