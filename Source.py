# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Create table to insert records

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS datamodeling.default.source_data
# MAGIC (
# MAGIC   order_id INT PRIMARY KEY,
# MAGIC     order_date DATE,
# MAGIC     customer_id INT,
# MAGIC     customer_name VARCHAR(100),
# MAGIC     customer_email VARCHAR(100),
# MAGIC     product_id INT,
# MAGIC     product_name VARCHAR(100),
# MAGIC     product_category VARCHAR(50),
# MAGIC     quantity INT,
# MAGIC     unit_price DECIMAL(10, 2),
# MAGIC     payment_type VARCHAR(50),
# MAGIC     country VARCHAR(50),
# MAGIC     last_updated DATE
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC Insert records 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO datamodeling.default.source_data VALUES 
# MAGIC (1001, '2024-07-01', 1, 'Alice Johnson', 'alice@gmail.com', 501, 'iPhone 14', 'Electronics', 1, 999.99, 'Credit Card', 'USA', '2024-07-01'),
# MAGIC (1002, '2024-07-01', 2, 'Bob Smith', 'bob@yahoo.com', 502, 'AirPods Pro', 'Electronics', 2, 199.99, 'PayPal', 'USA', '2024-07-01'),
# MAGIC (1003, '2024-07-01', 3, 'Charlie Brown', 'charlie@outlook.com', 503, 'Nike Shoes', 'Footwear', 1, 129.99, 'Credit Card', 'Canada', '2024-07-01');

# COMMAND ----------

# MAGIC %md
# MAGIC Insert records for incremental load

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Incremental Load
# MAGIC INSERT INTO datamodeling.default.source_data VALUES 
# MAGIC (1004, '2024-07-02', 4, 'David Lee', 'david@abc.com', 504, 'Samsung S23', 'Electronics', 1, 899.99, 'Credit Card', 'USA', '2024-07-02'),
# MAGIC (1005, '2024-07-02', 1, 'Alice Johnson', 'alice@gmail.com', 503, 'Nike Shoes', 'Footwear', 2, 129.99, 'Credit Card', 'USA', '2024-07-02');