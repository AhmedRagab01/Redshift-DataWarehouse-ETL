
### Project Overview
Sparkify is a growing music streaming startup, is scaling its infrastructure and migrating its user and song databases to the cloud. Their data is currently stored in AWS S3 in JSON format, containing both user activity logs and song metadata.

---

**Task**:<br>

The goal is to design and implement a robust ETL (Extract, Transform, Load) pipeline that:

- Extracts raw data from S3.
- Stages the data in Amazon Redshift using two staging tables.
- Transforms the staged data into a structured dimensional model.
- Loads the transformed data into fact and dimension tables, enabling the analytics team to gain insights into user behavior and song preferences.

---

**Database Schema**<br>

To support high-performance analytical queries, the Sparkify database is designed as a Star Schema Data Warehouse (DWH). This schema is optimized for fast aggregations and reporting by leveraging denormalization through the use of fact and dimension tables.

Sparkify Star Schema Design
- Fact Table: Stores the core business metrics (e.g., user song plays).
- Dimension Tables: Contain descriptive attributes (e.g., users, songs, artists, and time).
  
This schema structure enables efficient OLAP-style queries for Sparkify’s analytics team.

![Sparkify Database Star Schema](Redshift_Star_Schema.png "Sparkify Star Schema")

----
**ETL Pipeline**

The ETL pipeline is responsible for extracting raw JSON data from AWS S3, staging it in Amazon Redshift, and transforming it into structured tables for analytics. The process follows these steps:

1. Extract: Copy JSON data from S3 into staging tables in Redshift.
2. Transform: Use SQL SELECT statements to clean and structure the data.
3. Load: Insert the transformed data into the final analytical tables (fact & dimension tables).

This pipeline ensures a scalable and automated data workflow for Sparkify.

![Sparkify ETL Pipeline](ETL.png "AWS ETL")

---

**How to Run the Code ?!**

1. Writing Redshift DB Configurations in dwh.cfg
2. writing the iam role arn for redshift to access s3 in dwh.cfg
3. Writing Needed Tables Queries for Droping,Creation,Copying and Insertion.
4. Run Create_tables.py
5. Run ETL.py
---
**Example Queries**

Here is a query that retreives the hours with most number of songs plays.

![Example Query](example_query.png "Example Query")

