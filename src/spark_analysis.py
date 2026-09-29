import os
import sys

PYTHON = os.environ.get('PYSPARK_PYTHON', sys.executable)
os.environ['PYSPARK_PYTHON'] = PYTHON
os.environ['PYSPARK_DRIVER_PYTHON'] = PYTHON
os.environ.setdefault('JAVA_HOME', r'C:\Program Files\Java\jdk-17')
os.environ.setdefault('HADOOP_HOME', r'C:\hadoop')
os.environ['PATH'] = os.environ['PATH'] + os.pathsep + os.path.join(os.environ['HADOOP_HOME'], 'bin')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

MYSQL_URL = "jdbc:mysql://localhost:3306/finance_db?useSSL=false&serverTimezone=UTC&characterEncoding=utf8"
MYSQL_PROPS = {"user": "root", "password": os.environ.get("MYSQL_PASSWORD", ""), "driver": "com.mysql.cj.jdbc.Driver"}

def main():
    spark = SparkSession.builder \
        .appName("FinanceAnalysisSystem") \
        .master("local[*]") \
        .config("spark.driver.extraJavaOptions",
                "--add-opens=java.base/java.nio=ALL-UNNAMED "
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                "--add-opens=java.base/java.lang=ALL-UNNAMED "
                "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
                "--add-opens=java.base/java.io=ALL-UNNAMED "
                "--add-opens=java.base/java.util=ALL-UNNAMED "
                "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
                "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
                "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
                "--add-opens=java.base/java.net=ALL-UNNAMED "
                "--add-opens=java.base/sun.net.util=ALL-UNNAMED "
                "-Dio.netty.tryReflectionSetAccessible=true") \
        .config("spark.executor.extraJavaOptions",
                "--add-opens=java.base/java.lang=ALL-UNNAMED "
                "--add-opens=java.base/java.nio=ALL-UNNAMED "
                "--add-opens=java.base/java.net=ALL-UNNAMED "
                "--add-opens=java.base/java.util=ALL-UNNAMED "
                "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
                "-Dio.netty.tryReflectionSetAccessible=true") \
        .config("spark.jars", "libs/mysql-connector-j-8.0.33.jar") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.csv("data/financial_data.csv", header=True, inferSchema=True)
    print(f">>> 读取数据：{df.count()} 条")

    result = df.withColumn("RiskLevel",
        when(col("DebtAssetRatio") > 0.80, "极高风险")
        .when(col("DebtAssetRatio") > 0.70, "高风险")
        .when(col("DebtAssetRatio") > 0.50, "中等风险")
        .otherwise("稳健")
    ).dropna(subset=["DebtAssetRatio", "NetProfitMargin", "ROE", "ROA"])

    print(">>> 指标计算完成，写入 MySQL...")
    result.write.jdbc(MYSQL_URL, "finance_analysis", mode="overwrite", properties=MYSQL_PROPS)
    print(">>> 写入完成")

    result.show(20, truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()
