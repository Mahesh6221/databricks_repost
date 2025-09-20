Insurance Claims Data Pipeline

This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline with fraud detection rules applied on insurance claims data using Apache Spark. The workflow includes ingestion of raw datasets, data cleaning and validation, referential integrity checks, fraud rule application, and final analytical queries.

Setup Instructions

Prerequisites

1. Apache Spark (Databricks or local PySpark environment).

2. Access to /Volumes/workspace/default/ for reading and writing datasets.

3. Python 3.x environment with required Spark libraries.

Input Data:

1. Place the raw CSV files into the bronze layer:

/Volumes/workspace/default/bronze/customers.csv

/Volumes/workspace/default/bronze/claims.csv

2. Execution
Run the PySpark code step by step in a Databricks notebook (or any Spark-enabled environment).

Steps Followed
1. Data Ingestion

Loaded customers and claims datasets from the Bronze layer using spark.read.csv.

2. Data Quality & Integrity

i. Null Handling:
Dropped rows with nulls.

Filled missing values with defaults (Unknown, 0).

ii. Deduplication:

Identified duplicates using groupBy(...).count().

Removed duplicates in claims based on (customer_id, policy_id, claim_date, hospital_name).

Schema Validation:

iii. Ensured numeric fields (customer_id, claim_amount) were cast to appropriate types.

iv. Referential Integrity:

- Ensured claims were only linked to valid customers.

- Isolated invalid claims for further inspection.

- Stored cleaned data into the Silver layer (/Volumes/workspace/default/Silver_Dataset).

3. Fraud Detection Rules

Applied business rules on claims data:

Rule 1: Invalid Claim → claim_amount > insured_amount.

Rule 2: Suspicious Claim → More than 3 claims in 30 days.

Rule 3: Suspicious Claim → Claims filed in different states within the same week.

- Each claim was assigned a fraud status:

Invalid

Suspicious

Valid

Results stored in the Gold layer (/Volumes/workspace/default/Gold_Dataset).

4. Analytical SQL Queries

Ran business-driven insights on Gold data:

Top 5 customers with the most suspicious claims.

States with the highest suspicious claim ratio.

Average insured vs. claim amount for Valid vs. Suspicious claims.

Assumptions

Unique Identifiers:

customer_id uniquely identifies a customer.

claim_id uniquely identifies a claim.

Date Column:

claim_date is in a format that Spark can parse (yyyy-MM-dd).

Business Rules:

"30 days" is defined as a rolling window of 30×24 hours.

State mismatches are flagged only within the same week.

Data Storage:

Silver and Gold datasets are stored in parquet format for optimized reads/writes.

Challenges

Data Quality: Handling nulls in key fields (e.g., customer_id, claim_amount) required business-driven assumptions.

Window Functions: Implementing rolling 30-day checks efficiently in Spark required rangeBetween with timestamps.

Referential Integrity: Some claims lacked valid customer_id, raising questions about upstream data consistency.

Storage Paths: Both customers and claims were written to the same Silver/Gold directory, which may cause schema conflicts if not separated into subdirectories.

Outputs

Silver Dataset → Cleaned and validated data.

Gold Dataset → Fraud-detection enriched claims data.

Analytical Queries → Insights on suspicious activity.
