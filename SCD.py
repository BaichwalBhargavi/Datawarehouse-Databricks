# Databricks notebook source
# MAGIC %md
# MAGIC ## SCD type 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE datamodeling.default.scdtype1_source
# MAGIC (
# MAGIC   product_id int,
# MAGIC   product_name string,
# MAGIC   product_category string,
# MAGIC   processed_date DATE
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO  datamodeling.default.scdtype1_source
# MAGIC VALUES
# MAGIC (1,'prod1','cat1',current_date()),
# MAGIC (2,'prod2','cat2',current_date()),
# MAGIC (3,'prod3','cat3',current_date()),
# MAGIC (4,'prod4','cat4',current_date()),
# MAGIC (5,'prod5','cat5',current_date())

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a target table in gold layer

# COMMAND ----------

spark.sql("""SELECT * from datamodeling.default.scdtype1_source""").createOrReplaceTempView("src")


# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.scdtype1_table
# MAGIC (
# MAGIC   product_id int,
# MAGIC   product_name string,
# MAGIC   product_category string,
# MAGIC   processed_date DATE
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Perform UPSERT

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO datamodeling.gold.scdtype1_table t
# MAGIC USING datamodeling.default.scdtype1_source s
# MAGIC ON t.product_id = s.product_id
# MAGIC WHEN MATCHED and s.processed_date >= t.processed_date THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from datamodeling.gold.scdtype1_table

# COMMAND ----------

# MAGIC %md
# MAGIC Update the data of product 3 . Now it belongs to category 1

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE datamodeling.default.scdtype1_source
# MAGIC SET product_category ='cat1'
# MAGIC where product_id = 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD TYPE 2

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE datamodeling.default.scdtype2_source
# MAGIC (
# MAGIC   product_id int,
# MAGIC   product_name string,
# MAGIC   product_category string,
# MAGIC   processed_date DATE
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO  datamodeling.default.scdtype2_source
# MAGIC VALUES
# MAGIC (1,'prod1','cat1',current_date()),
# MAGIC (2,'prod2','cat2',current_date()),
# MAGIC (3,'prod3','cat3',current_date()),
# MAGIC (4,'prod4','cat4',current_date()),
# MAGIC (5,'prod5','cat5',current_date())

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.scdtype2_table
# MAGIC (
# MAGIC   product_id int,
# MAGIC   product_name string,
# MAGIC   product_category string,
# MAGIC   processed_date DATE,
# MAGIC   start_date DATE,
# MAGIC   end_date DATE,
# MAGIC   is_active string
# MAGIC )
# MAGIC

# COMMAND ----------

spark.sql("""
SELECT * ,
current_date() as start_date,
CAST('3000-01-01'as date) as end_date,
'Y' as is_active
from datamodeling.default.scdtype2_source""").createOrReplaceTempView("src2")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from src2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO datamodeling.gold.scdtype2_table t
# MAGIC USING src2 s
# MAGIC ON t.product_id = s.product_id AND
# MAGIC s.is_active = 'Y'
# MAGIC WHEN MATCHED and (
# MAGIC     t.product_name <> s.product_name or
# MAGIC     t.product_category <> s.product_category 
# MAGIC ) THEN UPDATE SET 
# MAGIC     t.end_date = current_date(),
# MAGIC     t.is_active = 'N'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from datamodeling.gold.scdtype2_table

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE into datamodeling.gold.scdtype2_table t
# MAGIC USING src2 s
# MAGIC ON t.product_id = s.product_id and 
# MAGIC t.is_active = 'Y'
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (  product_id,
# MAGIC   product_name,
# MAGIC   product_category,
# MAGIC   processed_date,
# MAGIC   start_date,
# MAGIC   end_date,
# MAGIC   is_active) VALUES (
# MAGIC   s.product_id,
# MAGIC   s.product_name,
# MAGIC   s.product_category,
# MAGIC   s.processed_date,
# MAGIC   s.start_date,
# MAGIC   s.end_date,
# MAGIC   s.is_active)  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from datamodeling.gold.scdtype2_table

# COMMAND ----------

# MAGIC %sql
# MAGIC Update datamodeling.default.scdtype2_source
# MAGIC SET product_category = 'cat1'
# MAGIC WHERE product_id = 4