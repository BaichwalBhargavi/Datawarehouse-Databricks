# Databricks notebook source
# MAGIC %md
# MAGIC ## DIM CUSTOMERS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.dimCustomers
# MAGIC SELECT *, row_number() over (order by customer_id) as dimCustomerKey
# MAGIC FROM 
# MAGIC (
# MAGIC   Select 
# MAGIC   Distinct(customer_id),customer_name,customer_email
# MAGIC   from datamodeling.silver.silver_table
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from datamodeling.gold.dimcustomers

# COMMAND ----------

# MAGIC %md
# MAGIC ## DIM Products

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.dimProducts
# MAGIC SELECT *, row_number() over (order by product_id) as dimProductKey
# MAGIC FROM 
# MAGIC (
# MAGIC   Select 
# MAGIC   Distinct(product_id),product_name,product_category
# MAGIC   from datamodeling.silver.silver_table
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from datamodeling.gold.dimProducts

# COMMAND ----------

# MAGIC %md
# MAGIC ## DIM PAYMENTS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.dimPayments
# MAGIC SELECT *, row_number() over (order by payment_type) as dimPaymentKey
# MAGIC FROM 
# MAGIC (
# MAGIC   Select 
# MAGIC   Distinct(payment_type),PAYMENT_METHOD
# MAGIC   from datamodeling.silver.silver_table
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from datamodeling.gold.dimPayments

# COMMAND ----------

# MAGIC %md
# MAGIC ## DIM REGIONS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.dimRegions
# MAGIC SELECT *, row_number() over (order by country) as dimRegionKey
# MAGIC FROM 
# MAGIC (
# MAGIC   Select 
# MAGIC   Distinct(country)
# MAGIC   from datamodeling.silver.silver_table
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from datamodeling.gold.dimRegions

# COMMAND ----------

# MAGIC %md
# MAGIC ## DIM ORDERS
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.dimOrders
# MAGIC SELECT 
# MAGIC row_number() over (order by order_id) as dimOrderKey,
# MAGIC   order_id,
# MAGIC  order_date,
# MAGIC  customer_id,
# MAGIC  customer_name,
# MAGIC  customer_email,
# MAGIC  product_id,
# MAGIC  product_name,
# MAGIC  product_category,
# MAGIC  payment_type,
# MAGIC  country,
# MAGIC  last_updated,
# MAGIC  PAYMENT_METHOD,
# MAGIC  PROCESSED_DATE
# MAGIC  from datamodeling.silver.silver_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from datamodeling.gold.dimOrders

# COMMAND ----------

# MAGIC %md
# MAGIC ## FACT TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.factSales
# MAGIC SELECT 
# MAGIC D.dimOrderKey,
# MAGIC C.dimCustomerKey,
# MAGIC R.dimRegionKey,
# MAGIC PY.dimPaymentKey,
# MAGIC P.dimProductKey,
# MAGIC F.quantity,
# MAGIC F.unit_price
# MAGIC
# MAGIC FROM datamodeling.silver.silver_table F 
# MAGIC LEFT JOIN 
# MAGIC datamodeling.gold.dimcustomers C
# MAGIC on F.customer_id = C.customer_id
# MAGIC LEFT JOIN 
# MAGIC datamodeling.gold.dimRegions R
# MAGIC on F.country = R.country
# MAGIC LEFT JOIN 
# MAGIC datamodeling.gold.dimPayments PY
# MAGIC on F.payment_type = PY.payment_type
# MAGIC LEFT JOIN 
# MAGIC datamodeling.gold.dimOrders D
# MAGIC on F.order_id = D.order_id
# MAGIC LEFT JOIN 
# MAGIC datamodeling.gold.dimProducts P
# MAGIC on F.product_id = P.product_id
# MAGIC     
# MAGIC