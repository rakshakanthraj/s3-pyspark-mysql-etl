# s3-pyspark-mysql-etl

An end-to-end PySpark ETL pipeline that ingests sales data from AWS S3, cleans and transforms it, handles bad files, and loads the processed data into MySQL data marts with SCD Type 2 logic.

---

## 🏗️ System Architecture & Workflow

```plaintext
+---------------------+       +-----------------------+       +------------------------+       +---------------------+

|     Amazon S3       | ----> |     PySpark Engine    | ----> |  SCD Type 2 Processing  | ----> |   MySQL Database    |
|   (Raw Sales CSV)   |       |   (Clean & Transform) |       |  (Handling Bad Files)  |       |    (Data Marts)     |
+---------------------+       +-----------------------+       +------------------------+       +---------------------+
```

* **Extraction:** PySpark safely extracts raw sales transactional data directly hosted on Amazon AWS S3 storage buckets.
* **Transformation & Validation:** Cleans input anomalies, isolates and logs bad data records, and prepares structured staging schemas.
* **SCD Type 2 Historical Loading:** Implements Slowly Changing Dimension (SCD) Type 2 tracking logic to preserve historical data changes inside destination target MySQL database marts.

---

## 📁 Repository Structure

```plaintext
s3-pyspark-mysql-etl/
├── resources/           # Multi-environment configs (dev/qa/prod) & SQL scripts
├── src/
│   ├── main/
│   │   ├── delete/      # Cleanup handlers (S3, database, local)
│   │   ├── download/    # S3 ingestion routines
│   │   ├── move/        # Staging file movement
│   │   ├── read/        # Data reading logic
│   │   ├── transformations/ # Spark jobs & mart creation
│   │   ├── upload/      # S3 write-back scripts
│   │   ├── utility/     # Spark/MySQL sessions, logging, encryption
│   │   └── write/       # MySQL & Parquet persistence
│   └── test/            # Data generators & scratchpads
├── .gitignore
└── README.md
```

---

## ⚡ Key Capabilities

* **Bad File and Record Handling:** Proactively captures and isolates structural mismatches or corrupted raw file records to prevent pipeline crashes.
* **SCD Type 2 Implementation:** Keeps track of dimension value movements over time by managing record validity windows (e.g., active flags, start dates, end dates).
* **Distributed Spark Transformations:** Uses memory-optimized distributed DataFrames to perform scale-out groupings, typecasting, and aggregations on underlying sales transactions.

---

## 🛠️ Prerequisites & Infrastructure

Ensure your local or cloud machine has the following software installed:
* **Python** (version 3.8 or higher)
* **Apache Spark** (version 3.x) configured with native cluster dependencies
* **MySQL Server** instance configuration
* **Spark JDBC Driver jar** (resolved automatically via execution or explicitly provided)

---

## 🚀 Deployment Instructions

### 1. Clone & Set Up Directory
```bash
git clone https://github.com/rakshakanthraj/s3-pyspark-mysql-etl.git
cd s3-pyspark-mysql-etl
```

### 2. Configure AWS S3 and JDBC Environment
Ensure your environmental runtime access context includes programmatic keys for AWS and local connection properties for your target MySQL schema.

### 3. Execution
Submit your core job script from the root workspace using the `spark-submit` application execution utility:
```bash
spark-submit --packages mysql:mysql-connector-java:8.0.33 src/main/transformations/jobs/main.py
```
