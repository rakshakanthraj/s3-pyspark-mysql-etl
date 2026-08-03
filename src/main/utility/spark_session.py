import findspark
findspark.init()
from pyspark.sql import SparkSession
from src.main.utility.logging_config import logger

def spark_session():
    jar_path = "/home/raksh/youtube_de_project1/jars/mysql-connector-java-8.0.26.jar"
    
    spark = SparkSession.builder.master("local[*]") \
        .appName("manish_spark2") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .getOrCreate()
        
    logger.info("spark session %s", spark)
    return spark