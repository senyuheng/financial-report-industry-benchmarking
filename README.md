# 上市公司财报行业横向对比分析

把 A 股 **100 万条财报记录**统一到可比口径，在 **24 个行业**之间做横向对比，
挑出指标显著偏离行业均值的公司。另外做了一次单机 vs 分布式对账实验，
回答一个具体问题：**这个量级到底该不该上分布式。**

数据规模：5 万家企业 × 24 个行业 × 5 年 × 4 个季度 = 100 万条。

---

## 结论先行

### 一、横向对比的难点在「口径」，不在「算力」

同样叫"毛利率"，银行和白酒不是一回事。所以顺序是**先统一口径、再谈聚合排序**：
营收同比 / 环比、毛利率、净利率的定义与裁剪值先定死，行业均值本身也要处理极端值，
否则一个异常样本就能把整个行业的基准带偏。

输出：24 个行业的可比聚合结果 + 指标显著偏离行业均值的企业清单。

### 二、该不该上分布式，先看数据规模

同一份数据跑两遍，一遍单机 Pandas、一遍 Spark on HDFS，同时监控耗时与内存：

| 方案 | 耗时 | 内存占用 |
| --- | --- | --- |
| Pandas（单机） | **0.48 s** | 基准 |
| Spark on HDFS | **16.3 s** | 仅为单机的 **0.2%** |

**这个量级下分布式不划算**：内存确实省了，但耗时长了 30 多倍，
启动与调度开销盖过了收益。分布式解决的是「数据量超出单机内存 / 需要横向扩展」，
不是「单机跑得慢」。选型看规模，不为技术而技术。

（原始输出见 `benchmark_result.json`，实验脚本 `src/benchmark.py`。）

## 架构

    Tushare Pro  ──►  清洗与口径统一  ──►  Spark on HDFS  ──►  MySQL / Parquet  ──►  可视化看板
     fetch_data.py    generate_data.py      spark_jobs/        spark_analysis.py     api/ + frontend/
                                           analysis.py

## 目录

    ├── src/
    │   ├── fetch_data.py      # Tushare 拉取真实财报（fina_indicator）
    │   ├── generate_data.py   # 从真实数据学习行业分布，按分布扩样到 100 万条
    │   ├── spark_analysis.py  # Spark 计算风险等级并写入 MySQL（单机版）
    │   ├── benchmark.py       # 单机 vs 分布式对账实验（上面结论二的来源）
    │   ├── smoke_test.py      # 最小可运行冒烟测试（5 家公司，验证环境）
    │   └── app.py             # Flask 看板（按行业 / 指标查询）
    ├── spark_jobs/analysis.py # Spark 聚合作业（4 个任务，结果写回 HDFS）
    ├── api/main.py            # FastAPI 接口层
    ├── frontend/index.html    # ECharts 前端
    ├── templates/             # Flask 模板
    ├── data/financial_data.csv# 真实样例数据（Tushare 拉取）
    └── benchmark_result.json  # 对账实验原始输出

## 运行

```bash
pip install -r requirements.txt

# 1) 取真实财报样本
export TUSHARE_TOKEN=你的token        # Windows cmd: set TUSHARE_TOKEN=你的token
python src/fetch_data.py

# 2) 按真实行业分布扩样到 100 万条（输出 data/finance_raw.parquet，已 gitignore）
python src/generate_data.py

# 3) 对账实验：Pandas vs Spark
python src/benchmark.py

# 4) 聚合结果写 MySQL
export MYSQL_PASSWORD=你的密码
python src/spark_analysis.py

# 5) 看板
python src/app.py         # Flask  → http://127.0.0.1:5000
uvicorn api.main:app      # FastAPI
```

> 分布式那几支（`spark_jobs/`、`api/`、`benchmark.py` 的 Spark 分支）需要
> Spark Standalone（`spark://localhost:7077`）+ HDFS（`hdfs://localhost:9000`）。
> 想先确认环境通不通，跑 `python src/smoke_test.py` 就够了。

## 数据说明

`data/financial_data.csv` 是从 Tushare `fina_indicator` 接口拉取的**真实**财报数据（小样本，
用于学习行业分布）。100 万条测试集是以真实分布为参数**扩样生成**的，不是真实企业清单 ——
目的是构造一个有真实分布特征、量级足够的测试集，用来验证分布式方案的性价比。
