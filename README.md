
### Project Overview
Project Overview
Sparkify, a growing music streaming startup, is scaling its infrastructure and migrating its user and song databases to the cloud. Their data is currently stored in AWS S3 in JSON format, containing both user activity logs and song metadata.

---

**Task**:<br>

The goal is to design and implement a robust ETL (Extract, Transform, Load) pipeline that:

- Extracts raw data from S3.
- Stages the data in Amazon Redshift using two staging tables.
- Transforms the staged data into a structured dimensional model.
- Loads the transformed data into fact and dimension tables, enabling the analytics team to gain insights into user behavior and song preferences.

---

**Database Schema**<br>

Building a Star Schema DWH that is optimized for heavy analytical Queries due to its denormalization nature with the use of Fact and Dimension Tables concept.

![Sparkify Database Star Schema](Redshift_Star_Schema.png "Sparkify Star Schema")

----
**ETL Pipeline**

The ETL Pipeline starts by Copying the JSON data from S3 into Staging Tables on Redshift, then Using a select statments on these tables to transform the data as needed then load it into Final Analytical Tables. 

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

