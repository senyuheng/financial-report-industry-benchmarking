import os
import sys
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, when

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ.setdefault('JAVA_HOME', r'C:\Program Files\Java\jdk-21')

plt.rcParams['font.sans-serif'] = ['SimHei'] 
plt.rcParams['axes.unicode_minus'] = False

def main():
    spark = SparkSession.builder \
        .appName("FinanceAnalysisSystem") \
        .master("local[*]") \
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED") \
        .getOrCreate()

    data = [
        ("600519", "贵州茅台", 1400.0, 300.0, 1500.0, 400.0),
        ("000858", "五粮液", 800.0, 250.0, 1000.0, 350.0),
        ("600036", "招商银行", 3500.0, 1200.0, 8000.0, 6500.0),
        ("000001", "平安银行", 1600.0, 600.0, 5000.0, 4200.0),
        ("601318", "中国平安", 9000.0, 4000.0, 20000.0, 15000.0)
    ]
    columns = ["StockCode", "CompanyName", "Revenue", "NetProfit", "TotalAssets", "TotalLiabilities"]
    
    df = spark.createDataFrame(data, columns)

    analysis_df = df.withColumn("DebtAssetRatio", round(col("TotalLiabilities") / col("TotalAssets"), 2)) \
                    .withColumn("NetProfitMargin", round(col("NetProfit") / col("Revenue"), 2))
    
    result_df = analysis_df.withColumn("RiskLevel", 
        when(col("DebtAssetRatio") > 0.7, "高风险").otherwise("稳健"))

    print(">>> 财报指标计算完成：")
    result_df.show()

    plot_data = result_df.select("CompanyName", "NetProfitMargin").toPandas()
    
    plt.figure(figsize=(10, 6))
    plt.bar(plot_data['CompanyName'], plot_data['NetProfitMargin'], color='skyblue')
    plt.title('上市公司净利率分析（Spark计算结果）')
    plt.xlabel('公司名称')
    plt.ylabel('净利率')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    plt.savefig('finance_analysis_preview.png')
    print(">>> 可视化图表已生成：finance_analysis_preview.png")

    spark.stop()

if __name__ == "__main__":
    main()