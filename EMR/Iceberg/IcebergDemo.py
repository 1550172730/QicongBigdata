from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType, LongType, StructType, StructField, StringType
from pyspark.sql.functions import col, lit
# from datetime import datetime

#    .config("spark.sql.catalog.allaniceberg.warehouse", "s3://allan-bigdata-test/iceberg_test") \

spark = SparkSession \
    .builder \
    .config("hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .config("spark.jars", "/usr/lib/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3-amzn-0.jar") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.allaniceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.allaniceberg.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager") \
    .config("spark.sql.catalog.glue_catalog.lock.table", "myIcebergLockTab") \
    .enableHiveSupport() \
    .getOrCreate()

# Variables
DB_NAME = "iceberg_test"
TABLE_NAME = "stu_iceberg"

#Create the customer table in Iceberg
spark.sql(f"""
    CREATE OR REPLACE TABLE allaniceberg.`{DB_NAME}`.`{TABLE_NAME}`(
        id             int,
        name           string,
        age        int
    )
    USING iceberg
    OPTIONS ('format-version'='2')
    """)

#LOCATION 's3://allan-bigdata-test/iceberg_test/{TABLE_NAME}'

#Insert data into customer table
spark.sql(f"""
    INSERT INTO allaniceberg.`{DB_NAME}`.`{TABLE_NAME}` 
values (1,'zs6',18),(2,'张思6',19)
    """)


#: org.apache.spark.SparkClassNotFoundException: [DATA_SOURCE_NOT_FOUND] Failed to find the data source: iceberg. Please find packages at `https://spark.apache.org/third-party-projects.html`.
#sudo cp /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.4.3-amzn-0.jar /usr/li
# b/spark/jars/