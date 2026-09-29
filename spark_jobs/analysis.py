import sys
import os, time
os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
os.environ.setdefault('PYSPARK_DRIVER_PYTHON', sys.executable)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, year, count, round as spark_round, rank
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("FinanceAnalysis") \
    .master("spark://localhost:7077") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 读取 HDFS 数据
df = spark.read.parquet("hdfs://localhost:9000/finance/raw/finance_raw.parquet")
df = df.repartition(16)
df.cache()
print(f">>> 数据总量：{df.count():,} 条")

# 任务1：各行业年度总营收、平均净利润率
t1 = time.time()
task1 = df.groupBy("industry", "year") \
    .agg(spark_round(sum("revenue")/1e8, 2).alias("总营收亿"),
         spark_round(avg(col("profit")/col("revenue")*100), 2).alias("平均净利润率%")) \
    .orderBy("industry", "year")
task1.write.mode("overwrite").parquet("hdfs://localhost:9000/finance/results/task1")
print(f">>> 任务1完成，耗时 {time.time()-t1:.1f}s")

# 任务2：异常值检测（利润为负但营收极高）
t2 = time.time()
revenue_threshold = df.approxQuantile("revenue", [0.9], 0.01)[0]
task2 = df.filter((col("profit") < 0) & (col("revenue") > revenue_threshold)) \
    .select("company_id", "company_name", "industry", "year", "quarter", "revenue", "profit")
task2.write.mode("overwrite").parquet("hdfs://localhost:9000/finance/results/task2")
print(f">>> 任务2完成，异常企业数：{task2.count():,}，耗时 {time.time()-t2:.1f}s")

# 任务3：各行业近5年营收增长率
t3 = time.time()
yearly = df.groupBy("industry", "year").agg(sum("revenue").alias("total_revenue"))
from pyspark.sql.functions import lag
w = Window.partitionBy("industry").orderBy("year")
task3 = yearly.withColumn("prev_revenue", lag("total_revenue").over(w)) \
    .withColumn("增长率%", spark_round((col("total_revenue")-col("prev_revenue"))/col("prev_revenue")*100, 2)) \
    .dropna()
task3.write.mode("overwrite").parquet("hdfs://localhost:9000/finance/results/task3")
print(f">>> 任务3完成，耗时 {time.time()-t3:.1f}s")

# 任务4：各行业净利润 Top10 企业
t4 = time.time()
w2 = Window.partitionBy("industry").orderBy(col("avg_profit").desc())
task4 = df.groupBy("industry", "company_id", "company_name") \
    .agg(spark_round(avg("profit")/1e6, 2).alias("avg_profit")) \
    .withColumn("rank", rank().over(w2)) \
    .filter(col("rank") <= 10)
task4.write.mode("overwrite").parquet("hdfs://localhost:9000/finance/results/task4")
print(f">>> 任务4完成，耗时 {time.time()-t4:.1f}s")

spark.stop()
print(">>> 全部分析完成")
