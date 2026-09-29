import sys
import os, json
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from pyspark.sql import SparkSession, functions as F
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

app = FastAPI()

def get_spark():
    return SparkSession.builder.appName("FinanceAPI")         .master("spark://localhost:7077")         .config("spark.executor.memory", "1g")         .getOrCreate()

@app.get("/api/task1")
def task1():
    spark = get_spark()
    df = spark.read.parquet("hdfs://localhost:9000/finance/results/task1")
    return [r.asDict() for r in df.orderBy("industry","year").collect()]

@app.get("/api/task2")
def task2():
    spark = get_spark()
    df = spark.read.parquet("hdfs://localhost:9000/finance/results/task2")
    return [r.asDict() for r in df.limit(100).collect()]

@app.get("/api/task3")
def task3():
    spark = get_spark()
    df = spark.read.parquet("hdfs://localhost:9000/finance/results/task3")
    return [r.asDict() for r in df.orderBy("industry","year").collect()]

@app.get("/api/task4")
def task4():
    spark = get_spark()
    df = spark.read.parquet("hdfs://localhost:9000/finance/results/task4")
    return [r.asDict() for r in df.orderBy("industry","rank").collect()]

@app.get("/api/benchmark")
def benchmark():
    with open(os.path.join(BASE_DIR, 'benchmark_result.json')) as f:
        return json.load(f)

@app.get("/api/query")
def query(industry: str = None, year: int = None, company: str = None):
    spark = get_spark()
    df = spark.read.parquet("hdfs://localhost:9000/finance/raw/finance_raw.parquet")
    if industry:
        df = df.filter(df.industry == industry)
    if year:
        df = df.filter(df.year == year)
    if company:
        df = df.filter(df.company_name.contains(company))
    result = df.groupBy("industry","year","quarter")         .agg(F.sum("revenue").alias("revenue"), F.sum("profit").alias("profit"))         .orderBy("year","quarter").limit(200).collect()
    return [r.asDict() for r in result]

@app.get("/api/industries")
def industries():
    spark = get_spark()
    df = spark.read.parquet("hdfs://localhost:9000/finance/raw/finance_raw.parquet")
    return [r[0] for r in df.select("industry").distinct().orderBy("industry").collect()]

app.mount("/", StaticFiles(directory=os.path.join(BASE_DIR, 'frontend'), html=True), name="static")
