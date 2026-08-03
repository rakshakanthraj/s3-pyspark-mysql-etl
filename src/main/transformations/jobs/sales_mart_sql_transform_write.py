import re
import mysql.connector
from pyspark.sql.functions import (
    col, sum as _sum, concat_ws, desc, rank, 
    when, round as _round, lit, current_date
)
from pyspark.sql.window import Window
from resources.dev import config
from src.main.write.database_write import DatabaseWriter

def sales_mart_calculation_table_write(spark, final_sales_team_data_mart_df):
    # 1. Aggregation using groupBy
    final_sales_team_data_mart = final_sales_team_data_mart_df \
        .groupBy(
            "store_id", 
            "sales_person_id", 
            "sales_person_first_name", 
            "sales_person_last_name", 
            "sales_month"
        ) \
        .agg(_sum("total_cost").alias("total_sales_every_month")) \
        .select(
            "store_id",
            "sales_person_id",
            concat_ws(" ", col("sales_person_first_name"), col("sales_person_last_name")).alias("full_name"),
            "sales_month",
            "total_sales_every_month"
        )

    # 2. Window for Ranking (Rank 1 gets the 1% incentive)
    rank_window = Window.partitionBy("store_id", "sales_month").orderBy(desc("total_sales_every_month"))

    # 3. Apply Rank and Calculate Incentive
    final_sales_team_data_mart_table = final_sales_team_data_mart \
        .withColumn("rnk", rank().over(rank_window)) \
        .withColumn("incentive", when(col("rnk") == 1, col("total_sales_every_month") * 0.01).otherwise(lit(0))) \
        .withColumn("incentive", _round(col("incentive"), 2)) \
        .withColumn("total_sales", col("total_sales_every_month")) \
        .select("store_id", "sales_person_id", "full_name", "sales_month", "total_sales", "incentive")

    # 4. READ EXISTING ACTIVE DATA FROM MYSQL
    query = f"(SELECT store_id, sales_person_id, full_name, sales_month, total_sales, incentive FROM {config.sales_team_data_mart_table} WHERE is_current = 'Y') as active_sales"
    try:
        existing_data = spark.read.jdbc(url=config.url, table=query, properties=config.properties)
    except Exception as e:
        # Fallback if table does not exist yet on initial run
        existing_data = spark.createDataFrame([], final_sales_team_data_mart_table.schema)

    # 5. JOIN to compare New Data with Existing Data
    joined_sales_df = final_sales_team_data_mart_table.alias("new").join(
        existing_data.alias("old"),
        (col("new.sales_person_id") == col("old.sales_person_id")) &
        (col("new.sales_month") == col("old.sales_month")),
        "left"
    )

    # 6. FILTER for New Records OR Records where Total Sales changed
    records_to_insert = joined_sales_df.filter(
        col("old.sales_person_id").isNull() |
        (col("new.total_sales") != col("old.total_sales"))
    ).select(
        col("new.store_id"),
        col("new.sales_person_id"),
        col("new.full_name"),
        col("new.sales_month"),
        col("new.total_sales"),
        col("new.incentive")
    ) \
     .withColumn("eff_start_date", current_date()) \
     .withColumn("eff_end_date", lit("9999-12-31")) \
     .withColumn("is_current", lit("Y"))

    # Cache & calculate count once
    records_count = records_to_insert.count()

    # 7. WRITE TO MYSQL
    if records_count > 0:
        # Collect updated sales_person_id & sales_month pairs to update old records in MySQL
        modified_records = joined_sales_df.filter(
            col("old.sales_person_id").isNotNull() & 
            (col("new.total_sales") != col("old.total_sales"))
        ).select("old.sales_person_id", "old.sales_month").collect()

        # Update old active records in MySQL to 'N' before inserting new ones
        if modified_records:
            # Safely parse host and port from JDBC URL if keys are not explicitly in properties
            host_match = re.search(r"jdbc:mysql://([^:/]+):?(\d+)?/([^?]+)", config.url)
            db_host = host_match.group(1) if host_match else "localhost"
            db_port = int(host_match.group(2)) if host_match and host_match.group(2) else 3306
            db_name = host_match.group(3) if host_match else config.properties.get("database")

            conn = mysql.connector.connect(
                host=config.properties.get("host", db_host),
                port=config.properties.get("port", db_port),
                user=config.properties["user"],
                password=config.properties["password"],
                database=db_name
            )
            cursor = conn.cursor()
            
            update_query = f"""
                UPDATE {config.sales_team_data_mart_table}
                SET is_current = 'N', eff_end_date = CURRENT_DATE()
                WHERE sales_person_id = %s AND sales_month = %s AND is_current = 'Y'
            """
            
            # Batch execution using executemany
            update_data = [(row['sales_person_id'], row['sales_month']) for row in modified_records]
            cursor.executemany(update_query, update_data)
            
            conn.commit()
            cursor.close()
            conn.close()

        # Append new/updated records
        db_writer = DatabaseWriter(url=config.url, properties=config.properties)
        db_writer.write_dataframe(records_to_insert, config.sales_team_data_mart_table)
        print(f"Successfully processed {records_count} records into sales team data mart.")
    else:
        print("No new data or changes detected in sales team mart.")

    return "Success"