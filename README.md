Data Modeling & ETL Notes

This repository documents my learnings and practical work on data modeling, ETL pipelines, and data warehousing concepts.
I’ve implemented Bronze, Silver, and Gold layers, designed fact and dimension tables, and studied different schema models and Slowly Changing Dimensions (SCDs).

🔹 ETL Layers

Source (SRC) – Where the data is originally available.

Staging Layer (Raw) – Data is brought in from source.

Persistent Staging → maintains historical data.

Transient Staging → stores only new data.

Transform (Enriched) – Data is cleaned, enriched, and transformed.

Load / Serve (Curated) – Data is modeled and made ready for analytics.

Key Concepts

Incremental Load → Pulls only new data to save cost.

Naming Conventions → Raw, Enriched, Serve (on-prem).

Medallion Architecture (Cloud) →

Bronze → Raw data

Silver → Enriched data

Gold → Curated/consumable data

🔹 Fact Tables

Fact tables store quantitative, measurable business data (facts/measures).
Examples: Sales Amount, Quantity Sold, Revenue, Temperature.

Types of Fact Tables

Transactional Fact Table → Each row = one transaction.

Periodic Fact Table → Aggregated by a period (e.g., daily sales).

Accumulating Fact Table → Tracks a process/journey (order placed → shipped → delivered).

Factless Fact Table → No numeric values, only textual attributes (e.g., attendance tracking).

🔹 Dimension Tables

Dimension tables provide context to fact tables (descriptive attributes).
Examples: Customer, Product, Store, Date.

Surrogate keys are added (primary keys).

Fact tables are created after dimension tables.

Types of Dimensions

Conformed Dimensions → Shared across multiple fact tables (e.g., Product).

Degenerate Dimensions → Minimal info (e.g., only Product_ID).

Junk Dimensions → Combine low-cardinality flags/attributes (e.g., Yes/No, Male/Female).

Role-Playing Dimensions → Same dimension used in different roles (e.g., Order Date, Ship Date, Delivery Date from the same Date Dimension).

🔹 Schema Models

Star Schema → Facts connected directly to dimension tables.

Snowflake Schema → Dimensions are normalized into multiple related tables.

🔹 Slowly Changing Dimensions (SCD)

How dimensions handle changes over time:

Type 1 (Upsert) → Old value replaced with new value (no history maintained).

Type 2 → Maintains full history with new records.

Type 3 → Maintains limited history (e.g., current & previous value).



