from pyspark.sql.functions import col, sum as _sum, substring, concat_ws
from resources.dev import config
from src.main.write.database_write import DatabaseWriter

# Calculation for customer mart:
# Find out the customer total purchase every month and write the data into MySQL table
def customer_mart_calculation_table_write(spark, final_customer_data_mart_df):
    
    # 1. Aggregate monthly sales per customer using groupBy (Optimized over Window + distinct)
    final_customer_data_mart = final_customer_data_mart_df \
        .withColumn("sales_date_month", substring(col("sales_date"), 1, 7)) \
        .groupBy(
            "customer_id", 
            "first_name", 
            "last_name", 
            "address", 
            "phone_number", 
            "sales_date_month"
        ) \
        .agg(_sum("total_cost").alias("total_sales")) \
        .select(
            "customer_id",
            concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"),
            "address",
            "phone_number",
            "sales_date_month",
            "total_sales"
        )

    final_customer_data_mart.show(5)

    # 2. Write the aggregated data into MySQL customer_data_mart table
    db_writer = DatabaseWriter(url=config.url, properties=config.properties)
    db_writer.write_dataframe(final_customer_data_mart, config.customer_data_mart_table)

    return "Success"