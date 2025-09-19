from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from pyspark.sql.types import DoubleType

# 创建启用Hive支持的SparkSession
spark = (SparkSession.builder \
    .appName("Hive Glue Catalog Parquet to DataFrame Example") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .enableHiveSupport() \
    #.config("spark.executor.memory", "512m") \
    #.config("spark.driver.memory", "512m") \
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .getOrCreate())

try:
    # 设置要查询的表名 (Parquet格式)
    table_name = "hive_test.allan_sale_ods"

    print(f"开始从Glue数据目录读取Parquet格式表: {table_name}")

    # 使用Spark SQL从Glue数据目录读取表数据
    # 方法1: 使用SQL查询
    # df = spark.sql(f"SELECT * FROM {table_name}")

    # 方法2: 使用table函数 (Glue会自动处理Parquet格式)
    df = spark.table(table_name)  # Parquet格式不需要跳过header行

    # 方法3: 直接从S3读取Parquet文件 (如果需要更多控制Parquet解析选项)
    # s3_path = "s3://allan-bigdata-test/ODS/allan_sales_ods/"
    # df = spark.read.format("parquet") \
    #     .load(s3_path)

    print(f"成功从Glue数据目录读取Parquet格式表: {table_name}！")
    print(f"数据模式：")
    df.printSchema()
    print(f"前5行数据：")
    df.show(5, truncate=False)

    # 可以在这里添加数据处理逻辑
    # 例如：df = df.filter(df.column > value)
    # 将total_amount列转换为数字类型
    df = df.withColumn("total_amount", col("total_amount").cast(DoubleType()))
    
    # 按产品ID分组并计算销售总额
    saleByProductDF = df.groupBy("product_id").agg(sum("total_amount").alias("daily_sale_sum"))
    
    print("按产品ID分组并计算销售总额完成")
    print("数据示例：")
    saleByProductDF.show(50, truncate=False)
    
    # 将结果写入S3，使用Parquet格式
    try:
        s3_output_path = "s3://allan-bigdata-test/DWD/allan_sale_product/dt=2025-07-14/"
        print(f"开始将销售汇总数据写入S3 Parquet: {s3_output_path}")
        
        saleByProductDF.write \
            .mode("overwrite") \
            .parquet(s3_output_path)
            
        print(f"成功将销售汇总数据写入S3 Parquet: {s3_output_path}")
    except Exception as e:
        print(f"写入S3 Parquet失败: {str(e)}")
        
except Exception as e:
    print(f"从Glue数据目录读取Parquet格式表失败: {str(e)}")
finally:
    spark.stop()
