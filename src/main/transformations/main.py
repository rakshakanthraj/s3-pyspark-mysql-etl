import os
import sys
import datetime
import shutil
from functools import reduce

from resources.dev import config
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from src.main.utility.my_sql_session import *
from pyspark.sql.functions import concat_ws, lit, expr
from src.main.upload.upload_to_s3 import UploadToS3
from resources.dev.config import s3_customer_datamart_directory
from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.database_read import DatabaseReader
from src.main.read.aws_read import *
from src.main.utility.encrypt_decrypt import decrypt
from src.main.utility.spark_session import *
from src.main.write.parquet_writer import ParquetWriter
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from src.main.transformations.jobs.dimension_tables_join import dimensions_table_join
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.utility.my_sql_session import expire_old_records

# Decrypt the encrypted keys stored in config
aws_access_key = decrypt(config.aws_access_key)
aws_secret_key = decrypt(config.aws_secret_key)

s3_client_provider = S3ClientProvider(aws_access_key, aws_secret_key)
s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()
logger.info("List of Buckets: %s", response['Buckets'])

# Check local directory for existing CSV files
csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = csv_files.copy()

if csv_files:
    formatted_files = ", ".join([f"'{f}'" for f in total_csv_files])
    statement = (
        f"SELECT DISTINCT file_name FROM {config.database_name}.{config.staging_table} "
        f"WHERE file_name IN ({formatted_files}) AND status='I'"
    )
    logger.info(f"Dynamically created statement: {statement}")
    cursor.execute(statement)
    data = cursor.fetchall()

    if data:
        logger.info("Your last run failed. Please check the 'I' status records.")
    else:
        logger.info("No failed records found for these files. Proceeding...")
else:
    logger.info("No CSV files found in local directory. Skipping DB check.")

try:
    s3_reader = S3Reader()
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client, config.bucket_name, folder_path=folder_path)

    logger.info("Absolute path on S3 bucket for CSV files: %s", s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception("No data available to process")
except Exception as e:
    logger.error("Exited with error: %s", e)
    raise e

bucket_name = config.bucket_name
local_directory = config.local_directory

prefix = f"s3://{bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
logging.info(f"File paths available on S3 under {bucket_name} bucket: {file_paths}")

try:
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("Exited with error: %s", e)
    sys.exit()

all_files = os.listdir(local_directory)
logger.info(f"List of files present in local directory after download: {all_files}")

if all_files:
    csv_files = []
    error_files = []
    for file in all_files:
        if file.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory, file)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, file)))

    if not csv_files:
        logger.error("No CSV data available to process the request")
        raise Exception("No CSV data available to process the request")
else:
    logger.error("There is no data to process")
    raise Exception("There is no data to process")

logger.info("********************************** Listing Files *****************************")
logger.info("List of CSV files to process: %s", csv_files)

logger.info("************************** Creating Spark session **************************")
spark = spark_session()
logger.info("************************* Spark session created ****************************")

logger.info("************ Checking schema for data loaded in S3 *********************************")

correct_files = []
for data in csv_files:
    data_schema = spark.read.format("csv").option("header", "true").load(data).columns
    logger.info(f"Schema for {data}: {data_schema}")
    logger.info(f"Mandatory columns schema: {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"Missing columns: {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"No missing columns found for {data}")
        correct_files.append(data)

logger.info(f"********************************* Correct files: {correct_files}")
logger.info(f"********************************* Error files: {error_files}")
logger.info("********************************* Moving Error data if any *************")

error_folder_local_path = config.error_folder_path_local

if error_files:
    os.makedirs(error_folder_local_path, exist_ok=True)
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(error_folder_local_path, file_name)

            shutil.move(file_path, destination_path)
            logger.info(f"Moved '{file_name}' to '{destination_path}'")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix, file_name)
            logger.info(f"{message}")
        else:
            logger.error(f"'{file_path}' does not exist")

logger.info("*********************** Updating product_staging_table ***************************")

insert_statements = []
db_name = config.database_name
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")

if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statement = (
            f"INSERT INTO {db_name}.{config.product_staging_table} "
            f"(file_name, file_location, created_date, status) "
            f"VALUES ('{filename}', '{filename}', '{formatted_date}', 'A')"
        )
        insert_statements.append(statement)
    
    logger.info(f"Insert statements for staging table: {insert_statements}")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("*********** No files available to process ***********************")
    raise Exception("**************** No valid CSV files found ***************")

logger.info("******************* Staging table updated successfully ***************")
logger.info("******************* Fixing extra columns from source **************")

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_column", StringType(), True),
])

df_list = []
for data in correct_files:
    data_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(data)
    
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"Extra columns in source {data}: {extra_columns}")

    if extra_columns:
        data_df = data_df.withColumn("additional_column", concat_ws(",", *extra_columns)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id",
                    "price", "quantity", "total_cost", "additional_column")
    else:
        data_df = data_df.withColumn("additional_column", lit(None)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id",
                    "price", "quantity", "total_cost", "additional_column")

    df_list.append(data_df)

if df_list:
    final_df_to_process = reduce(lambda df1, df2: df1.union(df2), df_list)
else:
    final_df_to_process = spark.createDataFrame([], schema=schema)

logger.info("**************** Final Dataframe from source going to processing ************")
final_enriched_data = final_df_to_process.distinct()
final_enriched_data.show(5)

# Load dimension tables
database_client = DatabaseReader(config.url, config.properties)

logger.info("*********************** Loading dimension tables ********************")
customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)
product_table_df = database_client.create_dataframe(spark, config.product_table)
product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table)
sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)
store_table_df = database_client.create_dataframe(spark, config.store_table)

s3_customer_store_sales_df_join = dimensions_table_join(
    final_df_to_process, customer_table_df, store_table_df, sales_team_table_df
)

logger.info("***************** Final Enriched Joined Data *************************")
s3_customer_store_sales_df_join.show(5)

# Customer Data Mart
logger.info("******************* Writing Customer Data Mart ******************")
final_customer_data_mart_df = s3_customer_store_sales_df_join \
    .select("ct.customer_id", "ct.first_name", "ct.last_name", "ct.address",
            "ct.pincode", "phone_number", "sales_date", "total_cost")

parquet_writer = ParquetWriter("overwrite", "parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df, config.customer_data_mart_local_file)
logger.info(f"Customer data written locally to {config.customer_data_mart_local_file}")

s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.customer_data_mart_local_file)
logger.info(f"{message}")

# Sales Team Data Mart
logger.info("********************* Writing Sales Team Data Mart ********************")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join \
    .select("store_id", "sales_person_id", "sales_person_first_name", "sales_person_last_name",
            "store_manager_name", "manager_id", "is_manager", "sales_person_address", "sales_person_pincode",
            "sales_date", "total_cost",
            expr("SUBSTRING(sales_date,1,7) as sales_month"))

parquet_writer.dataframe_writer(final_sales_team_data_mart_df, config.sales_team_data_mart_local_file)
logger.info(f"Sales team data written locally to {config.sales_team_data_mart_local_file}")

s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.sales_team_data_mart_local_file)
logger.info(f"{message}")

# Partitioned Parquet write
final_sales_team_data_mart_df.write.format("parquet") \
    .option("header", "true") \
    .mode("overwrite") \
    .partitionBy("sales_month", "store_id") \
    .option("path", config.sales_team_data_mart_partitioned_local_file) \
    .save()

s3_prefix = "sales_partitioned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp()) * 1000
for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        local_file_path = os.path.join(root, file)
        relative_file_path = os.path.relpath(local_file_path, config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path, config.bucket_name, s3_key)

# Mart Calculations (Passing spark session to both)
logger.info("****************** Calculating Customer Monthly Purchases **********")
customer_mart_calculation_table_write(spark, final_customer_data_mart_df)  # FIXED: added `spark`
logger.info("*********************** Customer Mart calculation complete *********")

logger.info("***** Calculating Sales Team Monthly Billed Amounts ********************************")
sales_mart_calculation_table_write(spark, final_sales_team_data_mart_df)
logger.info("**************** Sales Mart calculation complete *************")

# Move processed files on S3 & delete local files
source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory

if correct_files:
    for file_path in correct_files:
        file_name = os.path.basename(file_path)
        message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix, file_name)
        logger.info(f"{message}")

logger.info("********** Deleting processed files from local disk **********")
delete_local_file(config.local_directory)
delete_local_file(config.customer_data_mart_local_file)
delete_local_file(config.sales_team_data_mart_local_file)
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("********** Deleted processed files from local disk **********")

# Update staging status in MySQL
update_statements = []
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements = (
            f"UPDATE {db_name}.{config.product_staging_table} "
            f"SET status = 'I', updated_date = '{formatted_date}' "
            f"WHERE file_name = '{filename}'"
        )
        update_statements.append(statements)

    logger.info(f"Update statements for staging table: {update_statements}")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()

    
    expire_old_records(config.sales_team_data_mart_table, "sales_person_id", "sales_month")
else:
    logger.info("***************** Errors occurred during execution *********")
    sys.exit()

input("Press enter to terminate execution... ")