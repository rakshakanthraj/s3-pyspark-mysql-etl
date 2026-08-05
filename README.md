# Retail Sales ETL Pipeline using PySpark, AWS S3 and MySQL

## Overview

This project demonstrates an end-to-end ETL pipeline built using **PySpark**. It reads retail sales transaction data from **AWS S3**, validates and cleans the data, separates invalid records, performs business transformations, implements **Slowly Changing Dimension (SCD Type 2)** logic, and loads the processed data into **MySQL** data marts.

The project is designed to simulate a real-world data engineering workflow, covering ingestion, transformation, historical tracking, and loading curated datasets for reporting and analytics.

---

## Project Objective

The objective of this project is to build a production-style ETL pipeline that processes retail sales data efficiently while maintaining historical dimension changes using SCD Type 2 and loading analytics-ready data into MySQL.

---

## Architecture

```text
                Raw Sales CSV Files
                       │
                       ▼
                  Amazon AWS S3
                       │
                       ▼
             PySpark Data Ingestion
                       │
                       ▼
        Data Cleaning & Validation
                       │
          ┌────────────┴────────────┐
          ▼                         ▼
     Valid Records             Bad Records
          │                         │
          ▼                         ▼
 Business Transformations      Error Logging
          │
          ▼
 Dimension & Fact Tables
          │
          ▼
 SCD Type 2 Implementation
          │
          ▼
        MySQL Data Marts
```

---

## Tech Stack

* Python
* PySpark
* Spark SQL
* AWS S3
* MySQL
* JDBC
* SQL

---

## Skills Demonstrated

* ETL Pipeline Development
* PySpark Data Processing
* AWS S3 Integration
* Spark SQL
* Data Cleaning & Validation
* Error Handling
* Slowly Changing Dimension (SCD Type 2)
* Fact & Dimension Data Modeling
* MySQL Data Loading

---

## Repository Structure

```text
s3-pyspark-mysql-etl/
├── resources/
│   ├── configuration
│   └── sql
│
├── src/
│   ├── main/
│   │   ├── delete/
│   │   ├── download/
│   │   ├── move/
│   │   ├── read/
│   │   ├── transformations/
│   │   ├── upload/
│   │   ├── utility/
│   │   └── write/
│   │
│   └── test/
│
├── README.md
└── .gitignore
```

---

## ETL Workflow

1. Read raw retail sales data from AWS S3.
2. Validate schema and identify invalid records.
3. Separate bad records from valid records.
4. Clean and transform the data using PySpark.
5. Build dimension tables.
6. Apply SCD Type 2 logic for historical tracking.
7. Generate fact tables.
8. Load processed data into MySQL data marts.

---

## Features

* Automated ingestion from AWS S3.
* Distributed data processing using PySpark.
* Data validation and bad record handling.
* Historical tracking using SCD Type 2.
* Fact and Dimension table generation.
* Loading transformed data into MySQL.
* Modular project structure for easier maintenance.

---

## SCD Type 2 Implementation

This project implements **Slowly Changing Dimension (SCD Type 2)** to preserve historical changes in dimension tables.

Each update creates a new version of the record while retaining previous versions using:

* Effective Start Date
* Effective End Date
* Active Flag

This allows historical reporting without overwriting previous business information.

---

## Error Handling

The pipeline validates incoming records before transformation.

Invalid records are:

* Captured
* Logged
* Isolated
* Excluded from downstream processing

allowing the ETL pipeline to continue without failure.

---

## Configuration

Before running the project, configure the following inside the configuration files located under the **resources** folder:

* AWS Access Key
* AWS Secret Key
* MySQL Host
* Database Name
* Username
* Password

---

## Prerequisites

Install:

* Python 3.8+
* Apache Spark 3.x
* MySQL Server
* MySQL JDBC Driver

---

## Running the Project

Clone the repository:

```bash
git clone https://github.com/rakshakanthraj/s3-pyspark-mysql-etl.git
cd s3-pyspark-mysql-etl
```

Run the ETL pipeline:

```bash
spark-submit \
--packages mysql:mysql-connector-java:8.0.33 \
src/main/transformations/jobs/main.py
```

---

## Output

The pipeline produces:

* Clean Dimension Tables
* Fact Tables
* Historical SCD Type 2 Records
* Bad Record Logs
* Analytics-ready MySQL Data Marts

---

## Business Value

This solution enables organizations to:

* Preserve historical customer and product information.
* Improve reporting accuracy.
* Reduce manual data processing.
* Produce analytics-ready datasets.

---

## Challenges Faced

* Handling invalid and corrupted input records.
* Implementing SCD Type 2 historical tracking.
* Designing reusable PySpark modules.
* Separating valid and bad records without interrupting pipeline execution.

---

## What I Learned

Through this project, I gained hands-on experience in:

* Building end-to-end ETL pipelines.
* Processing large datasets using PySpark.
* Reading data from AWS S3.
* Implementing Slowly Changing Dimension (SCD Type 2).
* Designing Fact and Dimension models.
* Loading curated data into MySQL.
* Structuring production-style data engineering projects.

---

## Future Improvements

* Add Apache Airflow orchestration.
* Containerize using Docker.
* Implement CI/CD.
* Add unit testing.
* Support incremental loading.
* Extend the pipeline using Delta Lake.

---

## Author

**Raksha GK**

Aspiring Data Engineer

GitHub: https://github.com/rakshakanthraj
