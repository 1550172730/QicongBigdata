from pyspark.shell import spark
from pyspark.sql import SparkSession

spark = (SparkSession.builder \
    .appName("MySQL to DataFrame Example") \
    .config("spark.jars", "/usr/lib/spark/jars/mysql-connector-j-8.0.33.jar") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .getOrCreate())

# MySQL 连接配置（替换为你的实际参数）
mysql_config = {
    "host": "allanaurora.cluster-c3erpjwvbxqj.us-east-2.rds.amazonaws.com",  # 数据库地址
    "port": "3306",  # 端口号
    "database": "allantest",  # 数据库名称
    "table": "equ_equity_member",  # 表名
    "user": "admin",  # 用户名
    "password": "tqw961110"  # 密码
}

# 构建 JDBC URL
jdbc_url = f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"

try:
    # 读取数据到 DataFrame
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", mysql_config["table"]) \
        .option("user", mysql_config["user"]) \
        .option("password", mysql_config["password"]) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()

    print("成功从MySQL读取数据！")
    print(f"数据模式：")
    df.printSchema()
    print(f"前5行数据：")
    df.show(5, truncate=False)

    s3_config = {
        "output_path": "s3://allan-bigdata-test/user/Spark/equ_equity_member/",
        "format": "parquet",  # 可选 parquet, csv, json 等
        "save_mode": "overwrite"  # 可选 overwrite, append, ignore 等
    }

    print(f"开始写入S3：")
    (df.write
     .format(s3_config["format"])
     .mode(s3_config["save_mode"])
     .option("compression", "snappy")
     .save(s3_config["output_path"]))

    print(f"数据写入S3成功：")
except Exception as e:
    print(e)
    print(f"读取MySQL数据失败: {str(e)}")
finally:
    spark.stop()